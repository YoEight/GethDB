mod bus;
mod grpc;
pub mod messages;
mod process;

use bus::new_bus;
use geth_mikoshi::index::{Lsm, LsmSettings};
use geth_mikoshi::storage::FileSystemStorage;
use geth_mikoshi::wal::chunks::ChunkBasedWAL;
use geth_mikoshi::wal::WALRef;
use std::path::PathBuf;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let (bus, mailbox) = new_bus(500);
    let storage = FileSystemStorage::new(PathBuf::from("./geth"))?;
    let index = Lsm::load(LsmSettings::default(), storage.clone())?;

    let wal = WALRef::new(ChunkBasedWAL::load(storage)?);
    index.rebuild(&wal)?;

    process::start(mailbox, wal, index);
    grpc::start_server(bus).await?;

    Ok(())
}
