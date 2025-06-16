pub use crate::options::Options;

mod domain;
mod names;
mod options;
mod process;

use opentelemetry::trace::TracerProvider;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{logs::SdkLoggerProvider, trace::SdkTracerProvider};
pub use process::{
    indexing::IndexClient,
    reading::{self, ReaderClient},
    start_process_manager, start_process_manager_with_catalog,
    writing::WriterClient,
    Catalog, CatalogBuilder, ManagerClient, Proc,
};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{prelude::*, EnvFilter};

pub async fn run(options: Options) -> eyre::Result<()> {
    let provider = init_telemetry(&options)?;

    let manager = start_process_manager(options).await?;

    manager.wait_for(Proc::Grpc).await?;
    manager.manager_exited().await;

    provider.shutdown()?;

    Ok(())
}

pub struct EmbeddedClient {
    tracer_provider: SdkTracerProvider,
    manager: ManagerClient,
}

impl EmbeddedClient {
    #[tracing::instrument(skip_all, fields(target = "embedded-client"))]
    pub async fn shutdown(self) -> eyre::Result<()> {
        tracing::debug!("before shutdown");
        self.manager.shutdown().await?;
        tracing::debug!("shutdown completed");
        tracing::debug!("before tracer provider shutdown");
        self.tracer_provider.force_flush()?;
        self.tracer_provider.shutdown()?;
        tracing::debug!("tracer provider shutdown completed");

        Ok(())
    }
}

pub async fn run_embedded(options: Options) -> eyre::Result<EmbeddedClient> {
    let tracer_provider = init_telemetry(&options)?;
    let manager = start_process_manager(options).await?;

    manager.wait_for(Proc::Grpc).await?;

    Ok(EmbeddedClient {
        tracer_provider,
        manager,
    })
}

fn init_telemetry(options: &Options) -> eyre::Result<SdkTracerProvider> {
    let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
        // .with_tonic()
        // .with_endpoint(options.telemetry_endpoint.clone())
        .with_http()
        .with_endpoint(format!(
            "{}/ingest/otlp/v1/traces",
            options.telemetry_endpoint
        ))
        .build()?;

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(otlp_exporter)
        .build();

    let tracer = tracer_provider.tracer("geth-engine");
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    // Set up log exporter for events
    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_endpoint(format!(
            "{}/ingest/otlp/v1/logs",
            options.telemetry_endpoint
        ))
        .build()?;

    let log_provider = SdkLoggerProvider::builder()
        .with_batch_exporter(log_exporter)
        .build();

    let filter = EnvFilter::new("info")
        .add_directive("hyper=off".parse()?)
        .add_directive("tonic=off".parse()?)
        .add_directive("h2=off".parse()?)
        .add_directive("reqwest=off".parse()?)
        .add_directive("geth_engine=debug".parse()?);

    // let fmt_layer = tracing_subscriber::fmt::layer()
    //     .with_file(true)
    //     .with_line_number(true)
    //     .with_target(true);

    // tracing_subscriber::registry().init()
    //     .with_env_filter(EnvFilter::new(
    //         // "pyro_runtime=debug",
    //         "geth_engine=debug,geth_client=debug,geth_client_tests=debug,pyro_runtime=debug",
    //     ))
    //     // .with_max_level(tracing::Level::DEBUG)
    //     .with_file(true)
    //     .with_line_number(true)
    //     .with_target(true)
    //     .init();

    tracing_subscriber::registry()
        .with(OpenTelemetryLayer::new(tracer).with_filter(filter))
        .with(OpenTelemetryTracingBridge::new(&log_provider))
        // .with(otel_log_layer.with_filter(filter))
        // .with(fmt_layer.with_filter(filter))
        .init();

    Ok(tracer_provider)
}
