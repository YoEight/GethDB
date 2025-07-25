use crate::metrics::configure_metrics;
pub use crate::options::Options;

mod domain;
mod metrics;
mod names;
mod options;
mod process;

use geth_mikoshi::{
    storage::Storage, wal::chunks::ChunkContainer, FileSystemStorage, InMemoryStorage,
};
use opentelemetry::{trace::TracerProvider, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    logs::SdkLoggerProvider, metrics::PeriodicReader, trace::SdkTracerProvider, Resource,
};
pub use process::{
    indexing::IndexClient,
    manager::{start_process_manager_with_catalog, Catalog, CatalogBuilder, ManagerClient},
    reading::{self, ReaderClient},
    start_process_manager,
    writing::WriterClient,
    Proc, RequestContext,
};
use tokio::sync::OnceCell;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{filter::filter_fn, layer::SubscriberExt};
use tracing_subscriber::{prelude::*, EnvFilter};

pub mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

static STORAGE: OnceCell<Storage> = OnceCell::const_new();
static CHUNK_CONTAINER: OnceCell<ChunkContainer> = OnceCell::const_new();

pub(crate) fn get_storage() -> Storage {
    STORAGE.get().unwrap().clone()
}

pub(crate) fn get_chunk_container() -> ChunkContainer {
    CHUNK_CONTAINER.get().unwrap().clone()
}

fn configure_storage(options: &Options) -> eyre::Result<Storage> {
    let storage = if options.db == "in_mem" {
        InMemoryStorage::new_storage()
    } else {
        FileSystemStorage::new_storage(options.db.as_str().into())?
    };

    storage.init()?;

    Ok(storage)
}

pub async fn run(options: Options) -> eyre::Result<()> {
    let client = run_embedded(&options).await?;

    // TODO - handle CTRL-C signal to properly flush telemetry data before exiting
    client.manager.manager_exited().await;
    client.handles.shutdown()?;

    Ok(())
}

#[derive(Clone)]
pub struct EmbeddedClient {
    handles: TelemetryHandles,
    manager: ManagerClient,
}

impl EmbeddedClient {
    #[tracing::instrument(skip_all, fields(target = "embedded-client"))]
    pub async fn shutdown(self) -> eyre::Result<()> {
        self.manager.shutdown().await?;
        self.handles.shutdown()?;

        Ok(())
    }

    pub fn manager(&self) -> &ManagerClient {
        &self.manager
    }
}

#[derive(Default, Clone)]
struct TelemetryHandles {
    traces: Option<SdkTracerProvider>,
    logs: Option<SdkLoggerProvider>,
    metrics: Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
}

impl TelemetryHandles {
    fn shutdown(self) -> eyre::Result<()> {
        if let Some(provider) = self.traces {
            provider.shutdown()?;
        }

        if let Some(provider) = self.logs {
            provider.shutdown()?;
        }

        if let Some(provider) = self.metrics {
            provider.shutdown()?;
        }

        Ok(())
    }
}

pub async fn run_embedded(options: &Options) -> eyre::Result<EmbeddedClient> {
    let handles = init_telemetry(options)?;
    configure_metrics();

    let storage = configure_storage(options)?;
    let container = ChunkContainer::load(storage)?;

    STORAGE
        .set(container.storage().clone())
        .expect("to always work");
    CHUNK_CONTAINER
        .set(container)
        .expect("expect to always work");

    let manager = start_process_manager(options.clone()).await?;

    if !options.disable_grpc {
        manager.wait_for(Proc::Grpc).await?;
    }

    Ok(EmbeddedClient { handles, manager })
}

fn init_telemetry(options: &Options) -> eyre::Result<TelemetryHandles> {
    let mut handles = TelemetryHandles::default();
    let mut tracer_layer = None;
    let mut log_layer = None;

    let resource = Resource::builder()
        .with_service_name("geth-engine")
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .with_attribute(KeyValue::new("service.target", built_info::TARGET))
        .with_attribute(KeyValue::new("service.arch", built_info::CFG_TARGET_ARCH))
        .with_attribute(KeyValue::new("service.family", built_info::CFG_FAMILY))
        .with_attribute(KeyValue::new("service.os", built_info::CFG_OS))
        .with_attribute(KeyValue::new(
            "service.commit_hash",
            built_info::GIT_COMMIT_HASH.unwrap_or("unknown"),
        ))
        .build();

    if !options.telemetry.disabled {
        tracer_layer = if let Some(endpoint) = options
            .telemetry
            .traces_endpoint
            .as_ref()
            .or(options.telemetry.endpoint.as_ref())
        {
            // TLS must be configured to use gRPC
            let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
                // .with_tonic()
                // .with_endpoint(options.telemetry_endpoint.clone())
                .with_http()
                .with_endpoint(format!("{endpoint}/ingest/otlp/v1/traces"))
                .build()?;

            let tracer_provider = SdkTracerProvider::builder()
                .with_batch_exporter(otlp_exporter)
                .with_resource(resource.clone())
                .build();

            let tracer = tracer_provider.tracer("geth-engine");
            opentelemetry::global::set_tracer_provider(tracer_provider.clone());

            handles.traces = Some(tracer_provider);

            Some(
                OpenTelemetryLayer::new(tracer)
                    .with_filter(filter_fn(|metadata| metadata.is_span())),
            )
        } else {
            None
        };

        log_layer = if let Some(endpoint) = options
            .telemetry
            .logs_endpoint
            .as_ref()
            .or(options.telemetry.endpoint.as_ref())
        {
            let log_exporter = opentelemetry_otlp::LogExporter::builder()
                .with_http()
                .with_endpoint(format!("{endpoint}/ingest/otlp/v1/logs"))
                .build()?;

            let log_provider = SdkLoggerProvider::builder()
                .with_batch_exporter(log_exporter)
                .with_resource(resource.clone())
                .build();

            handles.logs = Some(log_provider.clone());

            Some(OpenTelemetryTracingBridge::new(&log_provider))
        } else {
            None
        };

        if let Some(endpoint) = options
            .telemetry
            .metrics_endpoint
            .as_ref()
            .or(options.telemetry.endpoint.as_ref())
            .cloned()
        {
            let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_endpoint(format!("{endpoint}/v1/metrics"))
                .build()?;

            let reader = PeriodicReader::builder(metric_exporter)
                .with_interval(std::time::Duration::from_secs(
                    options.telemetry.metrics_collection_interval_in_secs,
                ))
                .build();

            let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
                .with_reader(reader)
                .with_resource(resource)
                .build();

            handles.metrics = Some(meter_provider.clone());

            opentelemetry::global::set_meter_provider(meter_provider);
        }
    }

    let fmt_layer = if options.telemetry.disabled || options.telemetry.endpoint.is_none() {
        let layer = tracing_subscriber::fmt::layer()
            .with_file(true)
            .with_line_number(true)
            .with_target(true);

        Some(layer)
    } else {
        None
    };

    tracing_subscriber::registry()
        .with(create_event_filter(options)?)
        .with(tracer_layer)
        .with(log_layer)
        .with(fmt_layer)
        .init();

    Ok(handles)
}

fn create_event_filter(options: &Options) -> eyre::Result<EnvFilter> {
    let ignore = vec!["hyper=off", "tonic=off", "h2=off", "reqwest=off"];
    let default_scopes = vec!["geth_engine=debug"];

    let mut filter = EnvFilter::new("info");

    for directive in ignore {
        filter = filter.add_directive(directive.parse()?);
    }

    for directive in default_scopes {
        filter = filter.add_directive(directive.parse()?);
    }

    for directive in &options.telemetry.event_filters {
        filter = filter.add_directive(directive.parse()?);
    }

    Ok(filter)
}
