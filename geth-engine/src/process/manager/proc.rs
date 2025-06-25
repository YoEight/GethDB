use std::sync::atomic::Ordering;

use geth_mikoshi::{storage::Storage, wal::chunks::ChunkContainer};
use tokio::{runtime::Handle, sync::mpsc::UnboundedReceiver};

use crate::{
    process::{
        manager::{catalog::Catalog, client::ManagerClient, Manager, ManagerCommand},
        ProcessEnv, Raw, RunningProc, Runtime,
    },
    Options, Proc,
};

pub async fn process_manager<S>(
    options: Options,
    client: ManagerClient,
    catalog: Catalog,
    container: ChunkContainer<S>,
    queue: UnboundedReceiver<ManagerCommand>,
) where
    S: Storage + Send + Sync + 'static,
{
    let closed = client.closed.clone();
    let mut manager = Manager {
        options,
        client,
        runtime: Runtime { container },
        catalog,
        proc_id_gen: 1,
        requests: Default::default(),
        closing: false,
        closed,
        close_resp: vec![],
        queue,
        processes_shutting_down: Default::default(),
    };

    while let Some(cmd) = manager.queue.recv().await {
        if let Err(e) = manager.handle(cmd) {
            tracing::error!("unexpected: {}", e);
            break;
        }

        if manager.closed.load(Ordering::Acquire) {
            break;
        }
    }

    tracing::info!("process manager terminated");
}
