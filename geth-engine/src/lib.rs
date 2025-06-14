pub use crate::options::Options;

mod domain;
mod names;
mod options;
mod process;

use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
pub use process::{
    indexing::IndexClient,
    reading::{self, ReaderClient},
    start_process_manager, start_process_manager_with_catalog,
    writing::WriterClient,
    Catalog, CatalogBuilder, ManagerClient, Proc,
};
use tracing_subscriber::{
    layer::{self, SubscriberExt},
    util::SubscriberInitExt,
    EnvFilter, Layer,
};

pub async fn run(options: Options) -> eyre::Result<()> {
    let manager = start_process_manager(options).await?;

    manager.wait_for(Proc::Grpc).await?;
    manager.manager_exited().await;

    Ok(())
}

fn init_telemetry(options: &Options) -> eyre::Result<()> {
    let resource = Resource::builder()
        .with_service_name("geth-engine")
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .build();

    let logs_endpoints = format!("{}/ingest/otlp/v1/logs", options.telemetry_endpoint);
    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_endpoint(logs_endpoints)
        .build()?;

    let processor = opentelemetry_sdk::logs::BatchLogProcessor::builder(log_exporter).build();

    let logger_provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_log_processor(processor);

    let otel_layer = opentelemetry_appender_log::OpenTelemetryLogBridge::new(&logger_provider);

    let filter = EnvFilter::new("info").add_directive("geth-engine=debug".parse()?);
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_thread_names(true)
        .with_target(true)
        .with_level(true)
        .with_filter(filter);

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    Ok(())
}
