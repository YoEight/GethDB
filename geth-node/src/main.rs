use std::path::PathBuf;

use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use geth_domain::{Lsm, LsmSettings};
use geth_mikoshi::storage::FileSystemStorage;
use geth_mikoshi::wal::chunks::ChunkBasedWAL;
use geth_mikoshi::wal::WALRef;

use crate::process::Processes;

mod bus;
mod grpc;
pub mod messages;
mod process;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let storage = FileSystemStorage::new(PathBuf::from("./geth"))?;
    let index = Lsm::load(LsmSettings::default(), storage.clone())?;

    let wal = WALRef::new(ChunkBasedWAL::load(storage)?);
    index.rebuild(&wal)?;

    let processes = Processes::new(wal, index);
    grpc::start_server(processes).await?;

    Ok(())
}
