use std::sync::Arc;

use futures::{pin_mut, Stream, StreamExt};
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use geth_common::generated::next::protocol;
use geth_common::generated::next::protocol::protocol_server::Protocol;
use geth_common::{
    Client, Operation, OperationIn, OperationOut, ProgramListed, Reply, StreamRead, Subscribe,
    SubscriptionEvent, UnsubscribeReason,
};

use crate::grpc::local::LocalStorage;

pub struct ProtocolImpl<C> {
    client: Arc<C>,
}

impl<C> ProtocolImpl<C> {
    pub fn new(client: C) -> Self {
        Self {
            client: Arc::new(client),
        }
    }
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
    output: UnboundedReceiver<eyre::Result<OperationOut>>,
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
                    return match output {
                        Err(e) => {
                            tracing::error!("server error: {:?}", e);
                            Err(Status::unavailable(e.to_string()))
                        }

                        Ok(out)=> {
                            Ok(Some(Msg::Server(out)))
                        }
                    }
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
    input: Streaming<protocol::OperationIn>,
) where
    C: Client + Send + Sync + 'static,
{
    let local_storage = LocalStorage::new();
    let (tx, out_rx) = mpsc::unbounded_channel();
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
                        run_operation(client.clone(), local_storage.clone(), tx.clone(), operation);
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

fn run_operation<C>(
    client: Arc<C>,
    local_storage: LocalStorage,
    tx: UnboundedSender<eyre::Result<OperationOut>>,
    input: OperationIn,
) where
    C: Client + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let stream = execute_operation(client, local_storage, input).await;

        pin_mut!(stream);
        while let Some(out) = stream.next().await {
            if tx.send(out).is_err() {
                break;
            }
        }
    });
}

async fn execute_operation<C>(
    client: Arc<C>,
    local_storage: LocalStorage,
    input: OperationIn,
) -> impl Stream<Item = eyre::Result<OperationOut>>
where
    C: Client + Send + Sync + 'static,
{
    async_stream::try_stream! {
        let correlation = input.correlation;
        match input.operation {
            Operation::AppendStream(params) => {
                let completed = client
                    .append_stream(&params.stream_name, params.expected_revision, params.events)
                    .await?;

                yield OperationOut {
                    correlation,
                    reply: Reply::AppendStreamCompleted(completed),
                };
            }

            Operation::DeleteStream(params) => {
                let completed = client
                    .delete_stream(&params.stream_name, params.expected_revision)
                    .await?;

                yield OperationOut {
                    correlation,
                    reply: Reply::DeleteStreamCompleted(completed),
                };
            }

            Operation::ReadStream(params) => {
                let mut stream = client.read_stream(
                    &params.stream_name,
                    params.direction,
                    params.revision,
                    params.max_count,
                ).await;

                let token = local_storage.new_cancellation_token(correlation).await;
                loop {
                    select! {
                        record = stream.next() => {
                            match record {
                                None => {
                                   yield OperationOut {
                                        correlation,
                                        reply: Reply::StreamRead(StreamRead::EndOfStream),
                                    };

                                    local_storage.complete(&correlation).await;
                                    break;
                                }

                                Some(record) => {
                                    yield OperationOut {
                                        correlation,
                                        reply: Reply::StreamRead(StreamRead::EventAppeared(record?)),
                                    };
                                }
                            }
                        }

                        _ = token.notified() => break,
                    }
                }
            }

            Operation::Subscribe(subscribe) => {
                let token = local_storage.new_cancellation_token(correlation).await;
                let mut stream = match subscribe {
                    Subscribe::ToStream(params) => {
                        client.subscribe_to_stream(&params.stream_name, params.start).await
                    }

                    Subscribe::ToProgram(params) => {
                        client.subscribe_to_process(&params.name, &params.source).await
                    }
                };

                loop {
                    select! {
                        event = stream.next() => {
                            match event {
                                None => {
                                    yield OperationOut {
                                        correlation,
                                        reply: Reply::SubscriptionEvent(SubscriptionEvent::Unsubscribed(UnsubscribeReason::Server)),
                                    };

                                    local_storage.complete(&correlation).await;
                                    break;
                                }

                                Some(event) => {
                                    yield OperationOut {
                                        correlation,
                                        reply: Reply::SubscriptionEvent(event?),
                                    };
                                }
                            }
                        }

                        _ = token.notified() => {
                            yield OperationOut {
                                correlation,
                                reply: Reply::SubscriptionEvent(SubscriptionEvent::Unsubscribed(UnsubscribeReason::User)),
                            };
                            break;
                        },
                    }
                }
            }

            Operation::ListPrograms(_) => {
                let programs = client.list_programs().await?;
                yield OperationOut {
                    correlation,
                    reply: Reply::ProgramsListed(ProgramListed { programs }),
                };
            }

            Operation::GetProgram(params) => {
                yield OperationOut {
                    correlation,
                    reply: Reply::ProgramObtained(client.get_program(params.id).await?),
                };
            }

            Operation::KillProgram(params) => {
                yield OperationOut {
                    correlation,
                    reply: Reply::ProgramKilled(client.kill_program(params.id).await?),
                };
            }

            Operation::Unsubscribe => {
                local_storage.cancel(&correlation).await;
            }
        }
    }
}
