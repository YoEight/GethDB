mod streams;

use crate::process::Processes;
use geth_common::protocol::streams::server::StreamsServer;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::WriteAheadLog;
use tonic::transport::{self, Server};

pub async fn start_server<WAL, S>(processes: Processes<WAL, S>) -> Result<(), transport::Error>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let addr = "127.0.0.1:2113".parse().unwrap();
    let streams = streams::StreamsImpl::new(processes);

    tracing::info!("GethDB is listening on {}", addr);

    Server::builder()
        .add_service(StreamsServer::new(streams))
        .serve(addr)
        .await?;

    Ok(())
}
