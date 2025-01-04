use bb8::Pool;
use bytes::{Buf, BufMut};
use futures::{pin_mut, Stream, StreamExt};
use geth_mikoshi::wal::LogEntry;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use geth_common::generated::next::protocol;
use geth_common::generated::next::protocol::protocol_server::Protocol;
use geth_common::{
    Direction, Operation, OperationIn, OperationOut, Position, Record, Reply, StreamRead,
    StreamReadError, Subscribe, SubscriptionEvent, UnsubscribeReason,
};

use crate::messages::ReadStreamCompleted;
use crate::process::grpc::local::LocalStorage;
use crate::process::reading::ReaderClient;
use crate::process::resource::BufferManager;
use crate::process::subscription::SubscriptionClient;
use crate::process::writing::WriterClient;
use crate::process::{ManagerClient, Proc};

pub struct ProtocolImpl {
    client: ManagerClient,
}

impl ProtocolImpl {
    pub fn new(client: ManagerClient) -> Self {
        Self { client }
    }
}

#[derive(Clone)]
struct Internal {
    writer: WriterClient,
    reader: ReaderClient,
    sub: SubscriptionClient,
    pool: Pool<BufferManager>,
}

async fn resolve_internal(mgr: ManagerClient) -> eyre::Result<Internal> {
    let writer_id = mgr.wait_for(Proc::Writing).await?;
    let reader_id = mgr.wait_for(Proc::Reading).await?;
    let sub_id = mgr.wait_for(Proc::PubSub).await?;
    let pool = mgr.pool.clone();

    Ok(Internal {
        writer: WriterClient::new(writer_id, mgr.clone()),
        reader: ReaderClient::new(reader_id, mgr.clone()),
        sub: SubscriptionClient::new(sub_id, mgr),
        pool,
    })
}

type Downstream = UnboundedSender<Result<protocol::OperationOut, Status>>;

#[tonic::async_trait]
impl Protocol for ProtocolImpl {
    type MultiplexStream = UnboundedReceiverStream<Result<protocol::OperationOut, Status>>;

    async fn multiplex(
        &self,
        request: Request<Streaming<protocol::OperationIn>>,
    ) -> Result<Response<Self::MultiplexStream>, Status> {
        let (tx, rx) = mpsc::unbounded_channel();

        let internal = match resolve_internal(self.client.clone()).await {
            Err(e) => return Err(Status::unavailable(e.to_string())),
            Ok(i) => i,
        };

        tokio::spawn(multiplex(internal, tx, request.into_inner()));

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

async fn multiplex(
    internal: Internal,
    downstream: Downstream,
    input: Streaming<protocol::OperationIn>,
) {
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
                        run_operation(
                            internal.clone(),
                            local_storage.clone(),
                            tx.clone(),
                            operation,
                        );
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

fn run_operation(
    internal: Internal,
    local_storage: LocalStorage,
    tx: UnboundedSender<eyre::Result<OperationOut>>,
    input: OperationIn,
) {
    tokio::spawn(async move {
        let stream = execute_operation(internal, local_storage, input).await;

        pin_mut!(stream);
        while let Some(out) = stream.next().await {
            if tx.send(out).is_err() {
                break;
            }
        }
    });
}

fn not_implemented<A>() -> eyre::Result<A> {
    eyre::bail!("not implemented");
}

async fn execute_operation(
    internal: Internal,
    local_storage: LocalStorage,
    input: OperationIn,
) -> impl Stream<Item = eyre::Result<OperationOut>> {
    async_stream::try_stream! {
        let correlation = input.correlation;
        match input.operation {
            Operation::AppendStream(params) => {
                let completed = internal.writer.append(params.stream_name, params.expected_revision, params.events).await?;

                yield OperationOut {
                    correlation,
                    reply: Reply::AppendStreamCompleted(completed),
                };
            }

            Operation::DeleteStream(_) => {
                not_implemented()?;
                // return Err::<_, eyre::Report>(eyre::eyre!("not implemented"));
                // let completed = client
                //     .delete_stream(&params.stream_name, params.expected_revision)
                //     .await?;

                // yield OperationOut {
                //     correlation,
                //     reply: Reply::DeleteStreamCompleted(completed),
                // };
            }

            Operation::ReadStream(params) => {
                let result = internal.reader.read(
                    &params.stream_name,
                    params.revision,
                    params.direction,
                    params.max_count as usize,
                ).await?;

                let mut stream = match result {
                    crate::messages::ReadStreamCompleted::StreamDeleted => {
                        yield OperationOut {
                            correlation,
                            // FIXME - report a proper stream deleted error.
                            reply: Reply::StreamRead(StreamRead::Error(StreamReadError)),
                        };

                        local_storage.complete(&correlation).await;
                        return;
                    }

                    crate::messages::ReadStreamCompleted::Unexpected(_) => {
                        yield OperationOut {
                            correlation,
                            // FIXME - report an error properly.
                            reply: Reply::StreamRead(StreamRead::Error(StreamReadError)),
                        };

                        local_storage.complete(&correlation).await;
                        return;
                    }

                    crate::messages::ReadStreamCompleted::Success(streaming) => streaming,
                };

                let token = local_storage.new_cancellation_token(correlation).await;
                loop {
                    select! {
                        outcome = stream.next() => {
                            match outcome {
                                Err(e) => {
                                    tracing::error!(target = correlation.to_string(), "{}", e);
                                    yield OperationOut {
                                        correlation,
                                        reply: Reply::StreamRead(StreamRead::Error(StreamReadError)),
                                    };

                                    local_storage.complete(&correlation).await;
                                    break;
                                }

                                Ok(entry) => {
                                    match entry {
                                        None => {
                                        yield OperationOut {
                                                correlation,
                                                reply: Reply::StreamRead(StreamRead::EndOfStream),
                                            };

                                            local_storage.complete(&correlation).await;
                                            break;
                                        }

                                        Some(entry) => {
                                            yield OperationOut {
                                                correlation,
                                                reply: Reply::StreamRead(StreamRead::EventAppeared(entry.into())),
                                            };
                                        }
                                    }
                                }
                            }
                        }

                        _ = token.notified() => break,
                    }
                }
            }

            Operation::Subscribe(subscribe) => {
                let token = local_storage.new_cancellation_token(correlation).await;
                let params = match subscribe {
                    Subscribe::ToStream(params) => {
                        params
                    }

                    Subscribe::ToProgram(_) => {
                        not_implemented()?
                        // client.subscribe_to_process(&params.name, &params.source).await
                    }
                };

                let mut position = 0u64;
                let mut catching_up = true;
                let mut history = Vec::<LogEntry>::new();
                let mut sub_stream = internal.sub.subscribe(&params.stream_name).await?;
                let mut read_stream = match internal
                    .reader
                    .read(&params.stream_name, params.start, Direction::Forward, usize::MAX)
                    .await?
                    {
                        ReadStreamCompleted::StreamDeleted => {
                            yield OperationOut {
                                correlation,
                                // FIXME - report a proper stream deleted error.
                                reply: Reply::StreamRead(StreamRead::Error(StreamReadError)),
                            };

                            local_storage.complete(&correlation).await;
                            return;
                        }

                        ReadStreamCompleted::Unexpected(_) => {
                            yield OperationOut {
                                correlation,
                                // FIXME - report a proper unexpected error.
                                reply: Reply::StreamRead(StreamRead::Error(StreamReadError)),
                            };

                            local_storage.complete(&correlation).await;
                            return;
                        }

                        ReadStreamCompleted::Success(stream) => stream,
                    };

                loop {
                    select! {
                        outcome = read_stream.next() => {
                            match outcome {
                                Err(e) => {
                                    tracing::error!(target = correlation.to_string(), "{}", e);
                                    yield OperationOut {
                                        correlation,
                                        reply: Reply::SubscriptionEvent(SubscriptionEvent::Unsubscribed(UnsubscribeReason::Server)),
                                    };

                                    local_storage.complete(&correlation).await;
                                    break;
                                }

                                Ok(entry) => {
                                    if let Some(entry) = entry {
                                        position = entry.position;
                                        yield OperationOut {
                                            correlation,
                                            reply: Reply::SubscriptionEvent(SubscriptionEvent::EventAppeared(entry.into())),
                                        };
                                    } else {
                                        catching_up = false;

                                        for entry in history.drain(..) {
                                            position = entry.position;
                                            yield OperationOut {
                                                correlation,
                                                reply: Reply::SubscriptionEvent(SubscriptionEvent::EventAppeared(entry.into())),
                                            };
                                        }
                                    }
                                }
                            }
                        }

                        outcome = sub_stream.next() => {
                            match outcome {
                                Err(e) => {
                                    tracing::error!(target = correlation.to_string(), "{}", e);
                                    yield OperationOut {
                                        correlation,
                                        reply: Reply::SubscriptionEvent(SubscriptionEvent::Unsubscribed(UnsubscribeReason::Server)),
                                    };

                                    local_storage.complete(&correlation).await;
                                    break;
                                }

                                Ok(entry) => {
                                    if let Some(entry) = entry {
                                        if entry.position <= position {
                                            continue;
                                        }

                                        if catching_up {
                                            history.push(entry);
                                            continue;
                                        }

                                        position = entry.position;
                                        yield OperationOut {
                                            correlation,
                                            reply: Reply::SubscriptionEvent(SubscriptionEvent::EventAppeared(entry.into())),
                                        };
                                    } else {
                                        yield OperationOut {
                                            correlation,
                                            reply: Reply::SubscriptionEvent(SubscriptionEvent::Unsubscribed(UnsubscribeReason::Server)),
                                        };

                                        local_storage.complete(&correlation).await;
                                        break;
                                    }
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
                not_implemented()?;
                // let programs = client.list_programs().await?;
                // yield OperationOut {
                //     correlation,
                //     reply: Reply::ProgramsListed(ProgramListed { programs }),
                // };
            }

            Operation::GetProgram(_) => {
                not_implemented()?;
                // yield OperationOut {
                //     correlation,
                //     reply: Reply::ProgramObtained(client.get_program(params.id).await?),
                // };
            }

            Operation::KillProgram(_) => {
                not_implemented()?;
                // yield OperationOut {
                //     correlation,
                //     reply: Reply::ProgramKilled(client.kill_program(params.id).await?),
                // };
            }

            Operation::Unsubscribe => {
                local_storage.cancel(&correlation).await;
            }
        };
    }
}
