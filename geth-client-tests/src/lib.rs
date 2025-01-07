#[cfg(test)]
mod append_read_tests;

#[cfg(test)]
mod delete_tests;

#[cfg(test)]
pub mod tests {
    use fake::{Dummy, Fake};
    use serde::{Deserialize, Serialize};
    use temp_dir::TempDir;

    use geth_common::EndPoint;
    use geth_engine::Options;
    use tracing_subscriber::EnvFilter;

    #[ctor::ctor]
    fn test_init() {
        let _ = tracing_subscriber::fmt::fmt()
            .with_env_filter(EnvFilter::new("geth_engine=debug"))
            // .with_max_level(tracing::Level::DEBUG)
            .with_file(true)
            .with_line_number(true)
            .with_target(true)
            .init();
    }

    pub fn random_valid_options(temp_dir: &TempDir) -> Options {
        Options {
            host: "127.0.0.1".to_string(),
            port: (1_113..2_113).fake(),
            db: temp_dir.path().as_os_str().to_str().unwrap().to_string(),
        }
    }

    pub fn client_endpoint(options: &Options) -> EndPoint {
        EndPoint {
            host: options.host.clone(),
            port: options.port,
        }
    }

    #[derive(Serialize, Deserialize, Dummy)]
    pub struct Toto {
        pub key: String,
        pub value: u64,
    }
}
