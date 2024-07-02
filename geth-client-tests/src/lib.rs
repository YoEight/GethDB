#[cfg(test)]
mod append_read_tests;

#[cfg(test)]
pub mod tests {
    use fake::Fake;
    use temp_dir::TempDir;

    use geth_common::EndPoint;
    use geth_engine::Options;

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
}
