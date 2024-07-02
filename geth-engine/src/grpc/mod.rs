use tonic::transport::{self, Server};

use geth_common::generated::next::protocol::protocol_server::ProtocolServer;
use geth_common::Client;

use crate::options::Options;

mod local;
mod protocol;

pub async fn start_server<C>(options: Options, client: C) -> Result<(), transport::Error>
where
    C: Client + Send + Sync + 'static,
{
    let addr = format!("{}:{}", options.host, options.port)
        .parse()
        .unwrap();

    let protocols = protocol::ProtocolImpl::new(client);

    tracing::info!("GethDB is listening on {}", addr);

    Server::builder()
        .add_service(ProtocolServer::new(protocols))
        .serve(addr)
        .await?;

    Ok(())
}
