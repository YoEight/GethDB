#[cfg(test)]
mod append_read_tests;

#[cfg(test)]
mod delete_tests;

#[cfg(test)]
mod program_tests;

#[cfg(test)]
pub mod tests {
    use fake::{Dummy, Fake};
    use serde::{Deserialize, Serialize};
    use temp_dir::TempDir;

    use geth_common::EndPoint;
    use geth_engine::Options;

    pub fn random_valid_options(temp_dir: &TempDir) -> Options {
        let mut opts = Options::new(
            "127.0.0.1".to_string(),
            (1_113..2_113).fake(),
            temp_dir.path().as_os_str().to_str().unwrap().to_string(),
        );

        opts.telemetry.event_filters = vec![
            "geth_client=debug".to_string(),
            "geth_client_tests=debug".to_string(),
            "pyro_runtime=debug".to_string(),
        ];

        opts
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
