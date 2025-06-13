use tonic::transport::Server;

use geth_common::generated::protocol::protocol_server::ProtocolServer;

use crate::process::ProcessEnv;

mod protocol;

pub async fn start_server(env: ProcessEnv) -> eyre::Result<()> {
    let client = env.client.clone();
    let addr = format!("{}:{}", env.options.host, env.options.port)
        .parse()
        .unwrap();

    let protocols = protocol::ProtocolImpl::connect(env.client).await?;

    tracing::info!(
        "GethDB is listening on {} using database '{}'",
        addr,
        env.options.db
    );

    Server::builder()
        .add_service(ProtocolServer::new(protocols))
        .serve_with_shutdown(addr, client.manager_exited())
        .await?;

    Ok(())
}

pub async fn run(mut env: ProcessEnv) -> eyre::Result<()> {
    // We don't really care about listening any incoming message from the outside.
    env.queue.close();
    start_server(env).await?;
    Ok(())
}
