use std::sync::Arc;

use futures::StreamExt;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::{Request, Response, Status, Streaming};
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

use geth_common::{Client, Operation, OperationIn, OperationOut, Reply};
use geth_common::generated::next::protocol;
use geth_common::generated::next::protocol::protocol_server::Protocol;

pub struct ProtocolImpl<C> {
    client: Arc<C>,
}

type Downstream = UnboundedSender<Result<protocol::OperationOut, Status>>;

#[tonic::async_trait]
impl<C> Protocol for ProtocolImpl<C>
where
    C: Client + Send + Sync + 'static,
{
    type MultiplexStream = UnboundedReceiverStream<Result<protocol::OperationOut, Status>>;

    async fn multiplex(
        &self,
        request: Request<Streaming<protocol::OperationIn>>,
    ) -> Result<Response<Self::MultiplexStream>, Status> {
        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(multiplex(self.client.clone(), tx, request.into_inner()));

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }
}

enum Msg {
    User(OperationIn),
    Server(OperationOut),
}

struct Pipeline {
    input: Streaming<protocol::OperationIn>,
    output: UnboundedReceiver<OperationOut>,
}

impl Pipeline {
    async fn recv(&mut self) -> Result<Option<Msg>, Status> {
        select! {
            input = self.input.next() => {
                if let Some(input) = input {
                    return match input {
                        Ok(operation) => Ok(Some(Msg::User(operation.into()))),
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

async fn multiplex<C>(
    client: Arc<C>,
    downstream: Downstream,
    mut input: Streaming<protocol::OperationIn>,
) where
    C: Client + Send + Sync + 'static,
{
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
                        run_operation(client.clone(), tx.clone(), operation);
                    }

                    Msg::Server(operation) => {
                        if downstream.send(Ok(operation.into())).is_err() {
                            tracing::warn!("user reset connection");
                            break;
                        }
                    }
                },
            },
        }
    }
}

fn run_operation<C>(client: Arc<C>, tx: UnboundedSender<OperationOut>, input: OperationIn)
where
    C: Client + Send + Sync + 'static,
{
    tokio::spawn(execute_operation(client, tx, input));
}

async fn execute_operation<C>(client: Arc<C>, tx: UnboundedSender<OperationOut>, input: OperationIn)
where
    C: Client + Send + Sync + 'static,
{
    let correlation = input.correlation;
    match input.operation {
        Operation::AppendStream(params) => {
            let outcome = client
                .append_stream(&params.stream_name, params.expected_revision, params.events)
                .await;

            let _ = tx.send(OperationOut {
                correlation,
                reply: match outcome {
                    Ok(r) => Reply::append_stream_completed_with_success(r),
                    Err(e) => todo!(),
                },
            });
        }

        Operation::DeleteStream(_) => {}
        Operation::ReadStream(_) => {}
        Operation::Subscribe(_) => {}
        Operation::ListPrograms(_) => {}
        Operation::GetProgram(_) => {}
        Operation::KillProgram(_) => {}
    }
}
