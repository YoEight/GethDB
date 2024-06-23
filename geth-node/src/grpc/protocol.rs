use futures::StreamExt;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
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

        tokio::spawn(multiplex(tx, request.into_inner()));

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }
}

enum Msg {
    User(OperationIn),
    Server(OperationOut),
}

struct Pipeline {
    input: Streaming<OperationIn>,
    output: UnboundedReceiver<OperationOut>,
}

impl Pipeline {
    async fn recv(&mut self) -> Result<Option<Msg>, Status> {
        select! {
            input = self.input.next() => {
                if let Some(input) = input {
                    return match input {
                        Ok(operation) => Ok(Some(Msg::User(operation))),
                        Err(e) => {
                            tracing::error!("user error: {:?}", e);
                            Err(e)
                        }
                    };
                }

                tracing::warn!("user closed connection");
                Ok(None)
            },

            output = self.output.recv() => {
                if let Some(output) = output {
                    return Ok(Some(Msg::Server(output)));
                }

                tracing::error!("unexpected server error");
                Err(Status::unavailable("unexpected server error"))
            }
        }
    }
}

async fn multiplex(downstream: Downstream, mut input: Streaming<OperationIn>) {
    let (tx, mut out_rx) = mpsc::unbounded_channel();
    let mut pipeline = Pipeline {
        input,
        output: out_rx,
    };

    loop {
        match pipeline.recv().await {
            Err(e) => {
                let _ = downstream.send(Err(e));
                break;
            }

            Ok(msg) => match msg {
                None => break,
                Some(msg) => match msg {
                    Msg::User(operation) => {
                        todo!();
                    }

                    Msg::Server(operation) => {
                        if downstream.send(Ok(operation)).is_err() {
                            tracing::warn!("user reset connection");
                            break;
                        }
                    }
                },
            },
        }
    }
}
