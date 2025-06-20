use std::sync::Arc;

use tokio::sync::Notify;
use tonic::transport::Server;

use geth_common::generated::protocol::protocol_server::ProtocolServer;
use tracing::instrument;

use crate::{process::ProcessEnv, ManagerClient, Options, Proc};

mod protocol;

pub async fn start_server(
    client: ManagerClient,
    options: Options,
    notify: Arc<Notify>,
) -> eyre::Result<()> {
    let addr = format!("{}:{}", options.host, options.port)
        .parse()
        .unwrap();

    let protocols = protocol::ProtocolImpl::connect(client).await?;

    tracing::info!(%addr, db = options.db, "GethDB is listening",);

    Server::builder()
        .add_service(ProtocolServer::new(protocols))
        .serve_with_shutdown(addr, notify.notified())
        .await?;

    Ok(())
}

#[instrument(skip_all, fields(host = env.options.host, port = env.options.port, proc = ?Proc::Grpc))]
pub async fn run(mut env: ProcessEnv) -> eyre::Result<()> {
    let notify = Arc::new(Notify::new());
    let handle = tokio::spawn(start_server(env.client, env.options, notify.clone()));

    while env.queue.recv().await.is_some() {
        // we don't care about any message from the process manager
    }

    shutdown(notify, handle).await;

    Ok(())
}

#[instrument(skip_all)]
async fn shutdown(notify: Arc<Notify>, handle: tokio::task::JoinHandle<eyre::Result<()>>) {
    tracing::info!("initate gRPC shutdown");
    notify.notify_one();
    let _ = handle.await;
    tracing::info!("completed");
}
