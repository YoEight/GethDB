mod bus;
mod grpc;
pub mod messages;
mod process;
pub mod types;
use bus::new_bus;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let (bus, mailbox) = new_bus(500);

    process::start(mailbox);
    let _ = grpc::start_server(bus).await;
}
