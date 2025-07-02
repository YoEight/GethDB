use clap::Parser;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let options = geth_engine::Options::parse();

    geth_engine::run(options).await
}
