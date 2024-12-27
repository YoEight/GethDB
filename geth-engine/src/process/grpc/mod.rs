use tonic::transport::{self, Server};

use geth_common::generated::next::protocol::protocol_server::ProtocolServer;
use geth_common::Client;

use crate::options::Options;
use crate::process::{ManagerClient, ProcessEnv};

mod local;
mod protocol;

pub async fn start_server<C>(
    options: Options,
    client: C,
    env: ProcessEnv,
) -> Result<(), transport::Error>
where
    C: Client + Send + Sync + 'static,
{
    let addr = format!("{}:{}", options.host, options.port)
        .parse()
        .unwrap();

    let protocols = protocol::ProtocolImpl::new(client, env);

    tracing::info!("GethDB is listening on {}", addr);

    Server::builder()
        .add_service(ProtocolServer::new(protocols))
        .serve(addr)
        .await?;

    Ok(())
}

pub async fn run(mut env: ProcessEnv) -> eyre::Result<()> {
    // We don't really care about listening any incoming message from the outside.
    env.queue.close();
    Ok(())
}
