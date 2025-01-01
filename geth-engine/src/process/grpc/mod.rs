use tonic::transport::{self, Server};

use geth_common::generated::next::protocol::protocol_server::ProtocolServer;

use crate::process::ProcessEnv;

mod local;
mod protocol;

pub async fn start_server(env: ProcessEnv) -> Result<(), transport::Error> {
    let addr = format!("{}:{}", env.options.host, env.options.port)
        .parse()
        .unwrap();

    let protocols = protocol::ProtocolImpl::new(env.client);

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
    start_server(env).await?;
    Ok(())
}
