use std::sync::Arc;
pub use crate::options::Options;

mod domain;
mod names;
mod options;
mod process;

use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{WithExportConfig};
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_opentelemetry::OpenTelemetryLayer;
pub use process::{
    indexing::IndexClient,
    reading::{self, ReaderClient},
    start_process_manager, start_process_manager_with_catalog,
    writing::WriterClient,
    Catalog, CatalogBuilder, ManagerClient, Proc,
};
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
    pub async fn shutdown(self) -> eyre::Result<()> {
        self.manager.shutdown().await?;
        self.manager.manager_exited().await;
        self.tracer_provider.shutdown()?;

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
        .with_endpoint(format!("{}/ingest/otlp/v1/traces", options.telemetry_endpoint))
        .build()?;

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(otlp_exporter)
        .build();

    let tracer = tracer_provider.tracer("geth-engine");
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    let filter = EnvFilter::new("info")
        .add_directive("hyper=off".parse()?)
        .add_directive("tonic=off".parse()?)
        .add_directive("h2=off".parse()?)
        .add_directive("reqwest=off".parse()?)
        .add_directive("geth_engine=debug".parse()?);

    tracing_subscriber::registry()
        .with(OpenTelemetryLayer::new(tracer).with_filter(filter))
        .init();

    Ok(tracer_provider)
}
