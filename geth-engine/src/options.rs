use clap::{Args, Parser};

#[derive(Args, Debug, Clone, Default)]
pub struct Telemetry {
    #[arg(
        long = "telemetry-disabled",
        default_value = "false",
        env = "GETH_TELEMETRY_DISABLED"
    )]
    pub disabled: bool,

    /// OpenTelemetry compatible endpoint where telemetry data is sent
    #[arg(long = "telemetry-endpoint", env = "GETH_TELEMETRY_ENDPOINT")]
    pub endpoint: Option<String>,

    /// OpenTelemetry compatible metrics endpoint where telemetry data is sent
    #[arg(
        long = "telemetry-metrics-endpoint",
        env = "GETH_TELEMETRY_METRICS_ENDPOINT"
    )]
    pub metrics_endpoint: Option<String>,

    #[arg(long = "telemetry-event-filters")]
    pub event_filters: Vec<String>,
}

#[derive(Parser, Debug, Clone)]
#[command(name = "geth-db")]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Options {
    /// Host IP address.
    #[arg(long, default_value = "127.0.0.1", env = "GETH_HOST")]
    pub host: String,

    /// Host Port.
    #[arg(long, default_value = "2113", env = "GETH_PORT")]
    pub port: u16,

    /// Data directory. If you want to use the in-memory storage, set this to `in_mem`
    #[arg(long, default_value = "./geth", env = "GETH_DB")]
    pub db: String,

    #[command(flatten)]
    pub telemetry: Telemetry,

    pub disable_grpc: bool,
}

impl Options {
    pub fn new(host: String, port: u16, db: String) -> Self {
        Self {
            host,
            port,
            db,
            telemetry: Telemetry::default(),
            disable_grpc: false,
        }
    }

    pub fn with_telemetry_sent_to_seq(self) -> Options {
        let telemetry = Telemetry::default();

        Self {
            telemetry: Telemetry {
                endpoint: Some("http://localhost:5341".to_string()),
                ..telemetry
            },
            ..self
        }
    }

    pub fn disable_telemetry(self) -> Self {
        let telemetry = Telemetry::default();

        Self {
            telemetry: Telemetry {
                disabled: true,
                ..telemetry
            },
            ..self
        }
    }

    pub fn disable_grpc(self) -> Self {
        Self {
            disable_grpc: true,
            ..self
        }
    }

    pub fn in_mem() -> Self {
        Self {
            db: "in_mem".to_string(),
            ..Self::default()
        }
    }

    pub fn in_mem_no_grpc() -> Self {
        let opts = Self::in_mem();

        Self {
            disable_grpc: true,
            ..opts
        }
    }
}

impl Default for Options {
    fn default() -> Self {
        Self::new("127.0.0.1".to_string(), 2_113, "./geth".to_string())
    }
}
