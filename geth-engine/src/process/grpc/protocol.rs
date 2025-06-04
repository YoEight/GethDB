use std::collections::VecDeque;

use futures::{pin_mut, Stream, StreamExt};
use geth_mikoshi::hashing::mikoshi_hash;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

use geth_common::generated::next::protocol;
use geth_common::generated::next::protocol::protocol_server::Protocol;
use geth_common::{
    Direction, GetProgramError, Operation, OperationIn, OperationOut, ProgramKilled, ProgramListed,
    ProgramObtained, ReadStreamCompleted, Record, Reply, StreamRead, Subscribe, SubscribeToStream,
    SubscriptionEvent, UnsubscribeReason,
};
use uuid::Uuid;

use crate::process::grpc::local::LocalStorage;
use crate::process::reading::ReaderClient;
use crate::process::subscription::SubscriptionClient;
use crate::process::writing::WriterClient;
use crate::process::{ManagerClient, Proc};
use crate::IndexClient;

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
    index: IndexClient,
}

async fn resolve_internal(mgr: ManagerClient) -> eyre::Result<Internal> {
    Ok(Internal {
        writer: mgr.new_writer_client().await?,
        reader: mgr.new_reader_client().await?,
        sub: mgr.new_subscription_client().await?,
        index: mgr.new_index_client().await?,
    })
}

type Downstream = UnboundedSender<Result<protocol::OperationOut, Status>>;

#[tonic::async_trait]
impl Protocol for ProtocolImpl {
    type MultiplexStream = UnboundedReceiverStream<Result<protocol::OperationOut, Status>>;

    async fn multiplex(
        &self,
        request: Request<tonic::Streaming<protocol::OperationIn>>,
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
    input: tonic::Streaming<protocol::OperationIn>,
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
    input: tonic::Streaming<protocol::OperationIn>,
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
                        let output = operation
                            .try_into()
                            .map_err(|e: eyre::Report| Status::unavailable(e.to_string()));
                        if downstream.send(output).is_err() {
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

fn unexpected_error<A>(report: eyre::Report) -> eyre::Result<A> {
    Err(report)
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
                let outcome = internal.writer.append(params.stream_name, params.expected_revision, params.events).await;

                let completed = match outcome {
                    Err(e) => {
                        yield OperationOut {
                            correlation,
                            reply: Reply::Error(e.to_string()),
                        };

                        return;
                    },

                    Ok(c) => c,
                };

                yield OperationOut {
                    correlation,
                    reply: Reply::AppendStreamCompleted(completed),
                };
            }

            Operation::DeleteStream(params) => {
                tracing::debug!("received deleting stream request");
                let completed = internal.writer.delete(params.stream_name, params.expected_revision).await?;
                tracing::debug!("delete stream request completed");

                yield OperationOut {
                    correlation,
                    reply: Reply::DeleteStreamCompleted(completed),
                };
            }

            Operation::ReadStream(params) => {
                let result = internal.reader.read(
                    &params.stream_name,
                    params.revision,
                    params.direction,
                    params.max_count as usize,
                ).await?;

                let mut stream = match result {
                    ReadStreamCompleted::StreamDeleted => {
                        yield OperationOut {
                            correlation,
                            reply: Reply::StreamRead(StreamRead::StreamDeleted),
                        };

                        local_storage.complete(&correlation).await;
                        return;
                    }

                    ReadStreamCompleted::Unexpected(e) => {
                        unexpected_error(e)?;
                        return;
                    }

                    ReadStreamCompleted::Success(streaming) => streaming,
                };

                let token = local_storage.new_cancellation_token(correlation).await;
                loop {
                    select! {
                        outcome = stream.next() => {
                            match outcome {
                                Err(e) => {
                                    yield OperationOut {
                                        correlation,
                                        reply: Reply::StreamRead(StreamRead::Unexpected(e)),
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
                                                reply: Reply::StreamRead(StreamRead::EventAppeared(entry)),
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
                match subscribe {
                    Subscribe::ToStream(params) => {
                        let mut catchup = if let Some(catchup) = CatchupSubscription::init(&internal, correlation, params).await? {
                            catchup
                        } else {
                            yield OperationOut {
                                correlation,
                                reply: Reply::StreamRead(StreamRead::StreamDeleted),
                            };

                            local_storage.complete(&correlation).await;
                            return;
                        };

                        loop {
                            select! {
                                outcome = catchup.next() => {
                                    match outcome {
                                        Err(e) => {
                                            tracing::error!(error = %e, stream = catchup.params.stream_name, "unexpected error when running catchup subscription");

                                            yield OperationOut {
                                                correlation,
                                                reply: Reply::SubscriptionEvent(SubscriptionEvent::Unsubscribed(UnsubscribeReason::Server)),
                                            };

                                            break;
                                        }

                                        Ok(out) => {
                                            if let Some(out) = out {
                                                yield out;
                                                continue;
                                            } else {
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
                                }
                            }
                        }

                        local_storage.complete(&correlation).await;
                    }

                    Subscribe::ToProgram(_) => {
                        not_implemented()?
                        // client.subscribe_to_process(&params.name, &params.source).await
                    }
                };
            }

            Operation::ListPrograms(_) => {
                let programs = internal.sub.list_programs().await?;
                yield OperationOut {
                    correlation,
                    reply: Reply::ProgramsListed(ProgramListed { programs }),
                };
            }

            Operation::GetProgramStats(params) => {
                let result = match internal.sub.program_stats(params.id).await? {
                    Some(stats) => ProgramObtained::Success(stats),
                    None => ProgramObtained::Error(GetProgramError::NotExists),
                };

                yield OperationOut {
                    correlation,
                    reply: Reply::ProgramObtained(result),
                };
            }

            Operation::KillProgram(params) => {
                internal.sub.program_stop(params.id).await?;
                yield OperationOut {
                    correlation,
                    reply: Reply::ProgramKilled(ProgramKilled::Success),
                };
            }

            Operation::Unsubscribe => {
                local_storage.cancel(&correlation).await;
            }
        };
    }
}

struct CatchupSubscription {
    params: SubscribeToStream,
    position: u64,
    catching_up: bool,
    history: VecDeque<Record>,
    end_revision: u64,
    done: bool,
    correlation: Uuid,
    read_stream: crate::process::reading::Streaming,
    sub_stream: crate::process::subscription::Streaming,
}

impl CatchupSubscription {
    async fn init(
        internal: &Internal,
        correlation: Uuid,
        params: SubscribeToStream,
    ) -> eyre::Result<Option<Self>> {
        let sub_stream = internal
            .sub
            .subscribe_to_stream(&params.stream_name)
            .await?;

        let mut read_stream = match internal
            .reader
            .read(
                &params.stream_name,
                params.start,
                Direction::Forward,
                usize::MAX,
            )
            .await?
        {
            ReadStreamCompleted::StreamDeleted => {
                return Ok(None);
            }

            ReadStreamCompleted::Unexpected(e) => {
                return Err(e);
            }

            ReadStreamCompleted::Success(stream) => stream,
        };

        let current_revision = internal
            .index
            .latest_revision(mikoshi_hash(&params.stream_name))
            .await?;

        if current_revision.is_deleted() {
            return Ok(None);
        }

        let mut end_revision = 0;

        if let Some(revision) = current_revision.revision() {
            end_revision = revision;
        } else {
            read_stream = crate::process::reading::Streaming::empty();
        }

        Ok(Some(Self {
            params,
            position: 0,
            catching_up: false,
            history: VecDeque::new(),
            done: false,
            correlation,
            end_revision,
            read_stream,
            sub_stream,
        }))
    }

    // CAUTION: a situation where an user is reading very far away from the head of the stream and while that stream is actively being writen on could lead
    // to uncheck memory usage as everything will be stored in the history buffer.
    //
    // TODO: Implement a mechanism to limit the size of the history buffer by implementing a backpressure mechanism.
    async fn next(&mut self) -> eyre::Result<Option<OperationOut>> {
        if self.done {
            return Ok(None);
        }

        if self.catching_up {
            loop {
                select! {
                    outcome = self.read_stream.next() => {
                        match outcome {
                            Err(e) => return Err(e),
                            Ok(outcome) => if let Some(event) = outcome {
                                return Ok(Some(OperationOut {
                                    correlation: self.correlation,
                                    reply: Reply::SubscriptionEvent(SubscriptionEvent::EventAppeared(event)),
                                }));
                            } else {
                                self.catching_up = false;
                                return Ok(Some(OperationOut {
                                    correlation: self.correlation,
                                    reply: Reply::SubscriptionEvent(SubscriptionEvent::CaughtUp),
                                }));
                            }
                        }
                    }

                    outcome = self.sub_stream.next() => {
                        match outcome {
                            Err(e) => return Err(e),
                            Ok(outcome) => if let Some(event) = outcome {
                                if event.revision <= self.end_revision {
                                    continue;
                                }

                                self.history.push_back(event);
                            } else {
                                self.done = true;
                                return Ok(Some(OperationOut {
                                    correlation: self.correlation,
                                    reply: Reply::SubscriptionEvent(SubscriptionEvent::Unsubscribed(UnsubscribeReason::Server)),
                                }));
                            }
                        }
                    }
                }
            }
        }

        if let Some(event) = self.history.pop_front() {
            return Ok(Some(OperationOut {
                correlation: self.correlation,
                reply: Reply::SubscriptionEvent(SubscriptionEvent::EventAppeared(event)),
            }));
        }

        if let Some(event) = self.sub_stream.next().await? {
            return Ok(Some(OperationOut {
                correlation: self.correlation,
                reply: Reply::SubscriptionEvent(SubscriptionEvent::EventAppeared(event)),
            }));
        }

        self.done = true;

        Ok(None)
    }
}

// async fn catchup_subscription<A>(params: SubscribeToStream) -> eyre::Result<A> {
//     let mut position = 0u64;
//     let mut catching_up = true;
//     let mut history = Vec::<Record>::new();
//     let mut sub_stream = internal
//         .sub
//         .subscribe_to_stream(&params.stream_name)
//         .await?;
//     let mut read_stream = match internal
//         .reader
//         .read(
//             &params.stream_name,
//             params.start,
//             Direction::Forward,
//             usize::MAX,
//         )
//         .await?
//     {
//         ReadStreamCompleted::StreamDeleted => {
//             yield OperationOut {
//                 correlation,
//                 // FIXME - report a proper stream deleted error.
//                 reply: Reply::StreamRead(StreamRead::StreamDeleted),
//             };

//             local_storage.complete(&correlation).await;
//             return;
//         }

//         ReadStreamCompleted::Unexpected(e) => {
//             unexpected_error(e)?;
//             return;
//         }

//         ReadStreamCompleted::Success(stream) => stream,
//     };

//     loop {
//         select! {
//             outcome = read_stream.next() => {
//                 match outcome {
//                     Err(e) => {
//                         tracing::error!(target = correlation.to_string(), "{}", e);
//                         yield OperationOut {
//                             correlation,
//                             reply: Reply::SubscriptionEvent(SubscriptionEvent::Unsubscribed(UnsubscribeReason::Server)),
//                         };

//                         local_storage.complete(&correlation).await;
//                         break;
//                     }

//                     Ok(entry) => {
//                         if let Some(record) = entry {
//                             position = record.position;
//                             yield OperationOut {
//                                 correlation,
//                                 reply: Reply::SubscriptionEvent(SubscriptionEvent::EventAppeared(record)),
//                             };
//                         } else {
//                             catching_up = false;

//                             for record in history.drain(..) {
//                                 position = record.position;
//                                 yield OperationOut {
//                                     correlation,
//                                     reply: Reply::SubscriptionEvent(SubscriptionEvent::EventAppeared(record)),
//                                 };
//                             }
//                         }
//                     }
//                 }
//             }

//             outcome = sub_stream.next() => {
//                 match outcome {
//                     Err(e) => {
//                         tracing::error!(target = correlation.to_string(), "{}", e);
//                         yield OperationOut {
//                             correlation,
//                             reply: Reply::SubscriptionEvent(SubscriptionEvent::Unsubscribed(UnsubscribeReason::Server)),
//                         };

//                         local_storage.complete(&correlation).await;
//                         break;
//                     }

//                     Ok(record) => {
//                         if let Some(record) = record {
//                             if record.position <= position {
//                                 continue;
//                             }

//                             if catching_up {
//                                 history.push(record);
//                                 continue;
//                             }

//                             position = record.position;
//                             yield OperationOut {
//                                 correlation,
//                                 reply: Reply::SubscriptionEvent(SubscriptionEvent::EventAppeared(record)),
//                             };
//                         } else {
//                             yield OperationOut {
//                                 correlation,
//                                 reply: Reply::SubscriptionEvent(SubscriptionEvent::Unsubscribed(UnsubscribeReason::Server)),
//                             };

//                             local_storage.complete(&correlation).await;
//                             break;
//                         }
//                     }
//                 }
//             }

//             _ = token.notified() => {
//                 yield OperationOut {
//                     correlation,
//                     reply: Reply::SubscriptionEvent(SubscriptionEvent::Unsubscribed(UnsubscribeReason::User)),
//                 };
//                 break;
//             },
//         }
//     }
// }
