use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tonic::{Request, Response, Status, Streaming};
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

use geth_common::generated::next::protocol::{OperationIn, OperationOut};
use geth_common::generated::next::protocol::protocol_server::Protocol;

pub struct ProtocolImpl;

type Downstream = UnboundedSender<Result<OperationOut, Status>>;

#[tonic::async_trait]
impl Protocol for ProtocolImpl {
    type MultiplexStream = UnboundedReceiverStream<Result<OperationOut, Status>>;

    async fn multiplex(
        &self,
        request: Request<Streaming<OperationIn>>,
    ) -> Result<Response<Self::MultiplexStream>, Status> {
        let (tx, rx) = mpsc::unbounded_channel();

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }
}

async fn multiplex(downstream: Downstream, mut input: Streaming<OperationIn>) {
    while let Some(operation) = input.next().await {
        match operation {
            Err(e) => {
                let _ = downstream.send(Err(Status::internal(e.to_string())));

                break;
            }

            Ok(operation) => {
                let correlation: Uuid = operation.correlation.unwrap().into();

                // match operation.operation.unwrap() {}
            }
        }
    }
}
