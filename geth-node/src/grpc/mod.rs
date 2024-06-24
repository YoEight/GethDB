use tonic::transport::{self, Server};

use geth_common::generated::next::protocol::protocol_server::ProtocolServer;
use geth_common::protocol::streams::server::StreamsServer;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::WriteAheadLog;

use crate::process::{InternalClient, Processes};

mod protocol;

pub async fn start_server<WAL, S>(processes: Processes<WAL, S>) -> Result<(), transport::Error>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let addr = "127.0.0.1:2113".parse().unwrap();
    let internal_client = InternalClient::new(processes);
    let protocols = protocol::ProtocolImpl::new(internal_client);

    tracing::info!("GethDB is listening on {}", addr);

    Server::builder()
        .add_service(ProtocolServer::new(protocols))
        .serve(addr)
        .await?;

    Ok(())
}
