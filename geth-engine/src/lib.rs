use std::path::PathBuf;
use std::sync::Arc;

use tokio::select;

use geth_domain::{Lsm, LsmSettings};
use geth_mikoshi::storage::FileSystemStorage;
use geth_mikoshi::wal::chunks::ChunkBasedWAL;
use geth_mikoshi::wal::WALRef;

use crate::domain::index::Index;
pub use crate::options::Options;
use crate::process::{InternalClient, Processes};

mod bus;
mod domain;
mod grpc;
mod messages;
mod names;
mod options;
mod process;
mod services;

pub async fn run(options: Options) -> eyre::Result<()> {
    let storage = FileSystemStorage::new(PathBuf::from(&options.db))?;
    let lsm = Lsm::load(LsmSettings::default(), storage.clone())?;
    let index = Index::new(lsm);
    let wal = WALRef::new(ChunkBasedWAL::load(storage.clone())?);
    let processes = Processes::new(wal, index.clone());
    let sub_client = processes.subscriptions_client().clone();
    let client = Arc::new(InternalClient::new(processes));
    let services = services::start(client.clone(), index, sub_client);

    select! {
        server = grpc::start_server(options, client) => {
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
