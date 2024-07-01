use std::path::PathBuf;
use std::sync::Arc;

use tokio::select;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use geth_domain::{Lsm, LsmSettings};
use geth_mikoshi::storage::FileSystemStorage;
use geth_mikoshi::wal::chunks::ChunkBasedWAL;
use geth_mikoshi::wal::WALRef;

use crate::domain::index::Index;
use crate::process::{InternalClient, Processes};

mod bus;
mod domain;
mod grpc;
pub mod messages;
mod names;
mod process;
mod services;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let storage = FileSystemStorage::new(PathBuf::from("./geth"))?;
    let lsm = Lsm::load(LsmSettings::default(), storage.clone())?;
    let index = Index::new(lsm);
    let wal = WALRef::new(ChunkBasedWAL::load(storage.clone())?);
    let processes = Processes::new(wal, index.clone());
    let sub_client = processes.subscriptions_client().clone();
    let client = Arc::new(InternalClient::new(processes));
    let services = services::start(client.clone(), index, sub_client);

    select! {
        server = grpc::start_server(client) => {
            if let Err(e) = server {
                tracing::error!("GethDB node gRPC module crashed: {}", e);
            }
        }

        _ = services.exited() => {
            tracing::info!("GethDB node terminated");
        }
    }

    Ok(())
}
