pub mod generated {
    pub mod google {
        pub mod rpc {
            include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
        }
    }

    pub mod geth {
        pub mod protocol {
            include!(concat!(env!("OUT_DIR"), "/event_store.client.rs"));
            pub mod server {
                include!(concat!(env!("OUT_DIR"), "/event_store.client.streams.rs"));
            }
        }
    }
}

mod streams;

use tonic::transport::{self, Server};
use generated::geth::protocol::server::streams_server::StreamsServer;

pub fn start_server() {
    let addr = "[::1]:2113".parse().unwrap();
    let streams = streams::StreamsImpl::default();

    tracing::info!("GethDB is listening on {}", addr);

    tokio::spawn(async move {
        Server::builder()
            .add_service(StreamsServer::new(streams))
            .serve(addr)
            .await?;

        Ok::<(), transport::Error>(())
    });
}
