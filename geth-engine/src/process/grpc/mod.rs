use std::sync::Arc;

use tokio::sync::Notify;
use tonic::transport::Server;

use geth_grpc::generated::protocol::protocol_server::ProtocolServer;
use tracing::instrument;

use crate::{
    metrics::get_metrics,
    process::{manager::ManagerClient, Managed, ProcessEnv},
    Options,
};

mod protocol;

pub async fn start_server(
    client: ManagerClient,
    options: Arc<Options>,
    notify: Arc<Notify>,
) -> eyre::Result<()> {
    let addr = format!("{}:{}", options.host, options.port)
        .parse()
        .unwrap();

    let metrics = get_metrics();
    let protocols = protocol::ProtocolImpl::connect(metrics, client).await?;

    tracing::info!(%addr, db = options.db, "GethDB is listening",);

    Server::builder()
        .add_service(ProtocolServer::new(protocols))
        .serve_with_shutdown(addr, notify.notified())
        .await?;

    Ok(())
}

#[instrument(skip_all, fields(host = env.options.host, port = env.options.port, proc = ?env.proc))]
pub async fn run(mut env: ProcessEnv<Managed>) -> eyre::Result<()> {
    let notify = Arc::new(Notify::new());
    let handle = tokio::spawn(start_server(
        env.client.clone(),
        env.options.clone(),
        notify.clone(),
    ));

    while env.recv().await.is_some() {
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
