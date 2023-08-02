mod streams;

use crate::bus::Bus;
use geth_common::protocol::streams::server::StreamsServer;
use tonic::transport::{self, Server};

pub async fn start_server(bus: Bus) -> Result<(), transport::Error> {
    let addr = "127.0.0.1:2113".parse().unwrap();
    let streams = streams::StreamsImpl::new(bus);

    tracing::info!("GethDB is listening on {}", addr);

    Server::builder()
        .add_service(StreamsServer::new(streams))
        .serve(addr)
        .await?;

    Ok(())
}
