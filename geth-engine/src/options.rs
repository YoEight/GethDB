use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "geth-db")]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Options {
    /// Host IP address.
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Host Port.
    #[arg(long, default_value = "2113")]
    pub port: u16,

    // Data directory. If you want to use the in-memory storage, set this to `in_mem`
    #[arg(long, default_value = "./geth")]
    pub db: String,
}

impl Options {
    pub fn new(host: String, port: u16, db: String) -> Self {
        Self { host, port, db }
    }

    pub fn in_mem() -> Self {
        Self {
            db: "in_mem".to_string(),
            ..Self::default()
        }
    }
}

impl Default for Options {
    fn default() -> Self {
        Self::new("127.0.0.1".to_string(), 2_113, "./geth".to_string())
    }
}
