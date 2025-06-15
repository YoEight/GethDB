#[cfg(test)]
mod append_read_tests;

#[cfg(test)]
mod delete_tests;

#[cfg(test)]
mod program_tests;

#[cfg(test)]
pub mod tests {
    use fake::{Dummy, Fake};
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::Resource;
    use serde::{Deserialize, Serialize};
    use temp_dir::TempDir;

    use geth_common::EndPoint;
    use geth_engine::Options;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{EnvFilter, Layer};

    #[ctor::ctor]
    fn test_init() {
        // let resource = Resource::builder()
        //     .with_service_name("geth-engine")
        //     .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        //     .build();
        //
        // let logs_endpoints = format!("{}/ingest/otlp/v1/logs", options.telemetry_endpoint);
        // let log_exporter = opentelemetry_otlp::LogExporter::builder()
        //     .with_http()
        //     .with_endpoint(logs_endpoints)
        //     .build()?;
        //
        // let processor = opentelemetry_sdk::logs::BatchLogProcessor::builder(log_exporter).build();
        //
        // let logger_provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
        //     .with_resource(resource)
        //     .with_log_processor(processor)
        //     .build();
        //
        // let filter_otel = EnvFilter::new("info")
        //     .add_directive("geth_engine=debug".parse()?)
        //     .add_directive("hyper=off".parse().unwrap())
        //     .add_directive("tonic=off".parse().unwrap())
        //     .add_directive("h2=off".parse().unwrap())
        //     .add_directive("reqwest=off".parse().unwrap());
        //
        // let otel_layer = layer::OpenTelemetryTracingBridge::new(&logger_provider).with_filter(filter_otel);
        //
        // let filter = EnvFilter::new("info").add_directive("geth_engine=debug".parse()?);
        // let fmt_layer = tracing_subscriber::fmt::layer()
        //     .with_file(true)
        //     .with_thread_names(true)
        //     .with_target(true)
        //     .with_level(true)
        //     .with_filter(filter);
        //
        // tracing_subscriber::registry()
        //     .with(fmt_layer)
        //     .with(otel_layer)
        //     .init();
        // tracing_subscriber::registry().init()
        // tracing_subscriber::fmt::fmt()
        //     .with_env_filter(EnvFilter::new(
        //         // "pyro_runtime=debug",
        //         "geth_engine=debug,geth_client=debug,geth_client_tests=debug,pyro_runtime=debug",
        //     ))
        //     // .with_max_level(tracing::Level::DEBUG)
        //     .with_file(true)
        //     .with_line_number(true)
        //     .with_target(true)
        //     .init();
    }

    pub fn random_valid_options(temp_dir: &TempDir) -> Options {
        Options::new(
            "127.0.0.1".to_string(),
            (1_113..2_113).fake(),
            temp_dir.path().as_os_str().to_str().unwrap().to_string(),
        )
    }

    pub fn client_endpoint(options: &Options) -> EndPoint {
        EndPoint {
            host: options.host.clone(),
            port: options.port,
        }
    }

    #[derive(Serialize, Deserialize, Dummy, Clone, PartialEq, Eq, Debug)]
    pub struct Toto {
        pub key: String,
        pub value: i64,
    }
}
