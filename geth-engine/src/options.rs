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

    // Data directory.
    #[arg(long, default_value = "./geth")]
    pub db: String,
}
