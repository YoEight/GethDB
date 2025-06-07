use std::collections::VecDeque;

use futures::{pin_mut, Stream, StreamExt};
use geth_common::protocol::protocol_server::Protocol;
use geth_common::protocol::{self, SubscribeResponse};
use geth_mikoshi::hashing::mikoshi_hash;
use tokio::select;
use tokio::sync::mpsc::{self, unbounded_channel};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;

use geth_common::{
    AppendStream, DeleteStream, Direction, GetProgramError, ListPrograms, Operation, OperationIn,
    OperationOut, ProgramKilled, ProgramListed, ProgramObtained, ReadStream, ReadStreamCompleted,
    ReadStreamResponse, Record, Reply, Subscribe, SubscribeToStream, SubscriptionConfirmation,
    SubscriptionEvent, UnsubscribeReason,
};
use tonic::{Request, Response, Status};

use crate::process::reading::ReaderClient;
use crate::process::subscription::SubscriptionClient;
use crate::process::writing::WriterClient;
use crate::process::ManagerClient;
use crate::IndexClient;

#[derive(Clone)]
pub struct ProtocolImpl {
    writer: WriterClient,
    reader: ReaderClient,
    sub: SubscriptionClient,
    index: IndexClient,
}

impl ProtocolImpl {
    pub async fn connect(client: ManagerClient) -> eyre::Result<Self> {
        Ok(Self {
            writer: client.new_writer_client().await?,
            reader: client.new_reader_client().await?,
            sub: client.new_subscription_client().await?,
            index: client.new_index_client().await?,
        })
    }
}

#[tonic::async_trait]
impl Protocol for ProtocolImpl {
    type ReadStreamStream = UnboundedReceiverStream<Result<protocol::ReadStreamResponse, Status>>;
    type SubscribeStream = UnboundedReceiverStream<Result<protocol::SubscribeResponse, Status>>;

    async fn append_stream(
        &self,
        request: Request<protocol::AppendStreamRequest>,
    ) -> Result<Response<protocol::AppendStreamResponse>, Status> {
        let params: AppendStream = request.into_inner().into();
        match self
            .writer
            .append(params.stream_name, params.expected_revision, params.events)
            .await
        {
            Err(e) => Err(Status::internal(e.to_string())),
            Ok(result) => Ok(Response::new(result.into())),
        }
    }

    async fn read_stream(
        &self,
        request: Request<protocol::ReadStreamRequest>,
    ) -> Result<Response<Self::ReadStreamStream>, Status> {
        let params: ReadStream = request.into_inner().into();

        match self
            .reader
            .read(
                &params.stream_name,
                params.revision,
                params.direction,
                params.max_count as usize,
            )
            .await
        {
            Err(e) => Err(Status::internal(e.to_string())),
            Ok(outcome) => match outcome {
                ReadStreamCompleted::StreamDeleted => {
                    Err(Status::failed_precondition("stream-deleted"))
                }

                ReadStreamCompleted::Success(mut stream) => {
                    let (sender, recv) = unbounded_channel();

                    tokio::spawn(async move {
                        while let Some(event) = stream.next().await? {
                            if sender
                                .send(Ok(ReadStreamResponse::EventAppeared(event)
                                    .try_into()
                                    .unwrap()))
                                .is_err()
                            {
                                break;
                            }
                        }

                        Ok::<_, eyre::Report>(())
                    });

                    Ok(Response::new(UnboundedReceiverStream::new(recv)))
                }
            },
        }
    }

    async fn delete_stream(
        &self,
        request: Request<protocol::DeleteStreamRequest>,
    ) -> Result<Response<protocol::DeleteStreamResponse>, Status> {
        let params: DeleteStream = request.into_inner().into();

        match self
            .writer
            .delete(params.stream_name, params.expected_revision)
            .await
        {
            Err(e) => Err(Status::internal(e.to_string())),
            Ok(result) => Ok(Response::new(result.into())),
        }
    }

    async fn subscribe(
        &self,
        request: Request<protocol::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let (sender, recv) = unbounded_channel::<Result<SubscribeResponse, Status>>();

        match request.into_inner().into() {
            Subscribe::ToStream(params) => {
                match CatchupSubscription::init(self, params).await {
                    Err(e) => return Err(Status::internal(e.to_string())),
                    Ok(catchup) => {
                        if let Some(mut catchup) = catchup {
                            tokio::spawn(async move {
                                loop {
                                    match catchup.next().await {
                                        Err(e) => {
                                            let _ =
                                                sender.send(Err(Status::internal(e.to_string())));

                                            break;
                                        }

                                        Ok(event) => {
                                            if let Some(event) = event {
                                                if sender.send(Ok(event.into())).is_err() {
                                                    tracing::debug!(
                                                        stream = catchup.params.stream_name,
                                                        "user disconnected from catchup subscription"
                                                    );

                                                    break;
                                                }
                                            } else {
                                                tracing::debug!(
                                                    stream = catchup.params.stream_name,
                                                    "server ended catchup subscription"
                                                );

                                                let _ = sender.send(Ok(
                                                    SubscriptionEvent::Unsubscribed(
                                                        UnsubscribeReason::Server,
                                                    )
                                                    .into(),
                                                ));

                                                break;
                                            }
                                        }
                                    }
                                }
                            });
                        } else {
                            return Err(Status::failed_precondition("stream-deleted"));
                        }
                    }
                };
            }

            Subscribe::ToProgram(params) => {
                match self
                    .sub
                    .subscribe_to_program(&params.name, &params.source)
                    .await
                {
                    Err(e) => return Err(Status::internal(e.to_string())),
                    Ok(mut stream) => {
                        tokio::spawn(async move {
                            loop {
                                match stream.next().await {
                                    Err(e) => {
                                        let _ = sender.send(Err(Status::internal(e.to_string())));
                                        break;
                                    }

                                    Ok(event) => {
                                        if let Some(event) = event {
                                            if sender
                                                .send(Ok(
                                                    SubscriptionEvent::EventAppeared(event).into()
                                                ))
                                                .is_err()
                                            {
                                                tracing::debug!(
                                                    name = params.name,
                                                    "user disconnected from catchup subscription"
                                                );

                                                break;
                                            }
                                        } else {
                                            tracing::debug!(
                                                name = params.name,
                                                "server ended program subscription"
                                            );

                                            let _ =
                                                sender.send(Ok(SubscriptionEvent::Unsubscribed(
                                                    UnsubscribeReason::Server,
                                                )
                                                .into()));

                                            break;
                                        }
                                    }
                                }
                            }
                        });
                    }
                }
            }
        };

        Ok(Response::new(UnboundedReceiverStream::new(recv)))
    }

    async fn list_programs(
        &self,
        _: Request<protocol::ListProgramsRequest>,
    ) -> Result<Response<protocol::ListProgramsResponse>, Status> {
        match self.sub.list_programs().await {
            Err(e) => Err(Status::internal(e.to_string())),
            Ok(programs) => Ok(Response::new(ProgramListed { programs }.into())),
        }
    }

    async fn program_stats(
        &self,
        _request: Request<protocol::ProgramStatsRequest>,
    ) -> Result<Response<protocol::ProgramStatsResponse>, Status> {
        // Not yet implemented
        Err(Status::unimplemented("program_stats is not implemented"))
    }

    async fn stop_program(
        &self,
        _request: Request<protocol::StopProgramRequest>,
    ) -> Result<Response<protocol::StopProgramResponse>, Status> {
        // Not yet implemented
        Err(Status::unimplemented("stop_program is not implemented"))
    }
}

enum Msg {
    User(OperationIn),
    Server(OperationOut),
}

// struct Pipeline {
//     input: tonic::Streaming<protocol::OperationIn>,
//     output: UnboundedReceiver<eyre::Result<OperationOut>>,
// }

// impl Pipeline {
//     async fn recv(&mut self) -> Result<Option<Msg>, Status> {
//         select! {
//             input = self.input.next() => {
//                 if let Some(input) = input {
//                     return match input {
//                         Ok(operation) => Ok(Some(Msg::User(operation.into()))),
//                         Err(e) => {
//                             tracing::error!("user error: {:?}", e);
//                             Err(e)
//                         }
//                     };
//                 }

//                 tracing::warn!("user closed connection");
//                 Ok(None)
//             },

//             output = self.output.recv() => {
//                 if let Some(output) = output {
//                     return match output {
//                         Err(e) => {
//                             tracing::error!("server error: {:?}", e);
//                             Err(Status::unavailable(e.to_string()))
//                         }

//                         Ok(out)=> {
//                             Ok(Some(Msg::Server(out)))
//                         }
//                     }
//                 }

//                 tracing::error!("unexpected server error");
//                 Err(Status::unavailable("unexpected server error"))
//             }
//         }
//     }
// }

// async fn multiplex(
//     internal: Internal,
//     downstream: Downstream,
//     input: tonic::Streaming<protocol::OperationIn>,
// ) {
//     let local_storage = LocalStorage::new();
//     let (tx, out_rx) = mpsc::unbounded_channel();
//     let mut pipeline = Pipeline {
//         input,
//         output: out_rx,
//     };

//     loop {
//         match pipeline.recv().await {
//             Err(e) => {
//                 let _ = downstream.send(Err(e));
//                 break;
//             }

//             Ok(msg) => match msg {
//                 None => break,
//                 Some(msg) => match msg {
//                     Msg::User(operation) => {
//                         run_operation(
//                             internal.clone(),
//                             local_storage.clone(),
//                             tx.clone(),
//                             operation,
//                         );
//                     }

//                     Msg::Server(operation) => {
//                         let output = operation
//                             .try_into()
//                             .map_err(|e: eyre::Report| Status::unavailable(e.to_string()));
//                         if downstream.send(output).is_err() {
//                             tracing::warn!("user reset connection");
//                             break;
//                         }
//                     }
//                 },
//             },
//         }
//     }
// }

// fn run_operation(
//     internal: Internal,
//     local_storage: LocalStorage,
//     tx: UnboundedSender<eyre::Result<OperationOut>>,
//     input: OperationIn,
// ) {
//     tokio::spawn(async move {
//         let stream = execute_operation(internal, local_storage, input).await;

//         pin_mut!(stream);
//         while let Some(out) = stream.next().await {
//             if tx.send(out).is_err() {
//                 break;
//             }
//         }
//     });
// }

// fn unexpected_error<A>(report: eyre::Report) -> eyre::Result<A> {
//     Err(report)
// }

// async fn execute_operation(
//     internal: Internal,
//     local_storage: LocalStorage,
//     input: OperationIn,
// ) -> impl Stream<Item = eyre::Result<OperationOut>> {
//     async_stream::try_stream! {
//         let correlation = input.correlation;
//         match input.operation {
//             Operation::AppendStream(params) => {
//                 let outcome = internal.writer.append(params.stream_name, params.expected_revision, params.events).await;

//                 let completed = match outcome {
//                     Err(e) => {
//                         yield OperationOut {
//                             correlation,
//                             reply: Reply::Error(e.to_string()),
//                         };

//                         return;
//                     },

//                     Ok(c) => c,
//                 };

//                 yield OperationOut {
//                     correlation,
//                     reply: Reply::AppendStreamCompleted(completed),
//                 };
//             }

//             Operation::DeleteStream(params) => {
//                 tracing::debug!("received deleting stream request");
//                 let completed = internal.writer.delete(params.stream_name, params.expected_revision).await?;
//                 tracing::debug!("delete stream request completed");

//                 yield OperationOut {
//                     correlation,
//                     reply: Reply::DeleteStreamCompleted(completed),
//                 };
//             }

//             Operation::ReadStream(params) => {
//                 let result = internal.reader.read(
//                     &params.stream_name,
//                     params.revision,
//                     params.direction,
//                     params.max_count as usize,
//                 ).await?;

//                 let mut stream = match result {
//                     ReadStreamCompleted::StreamDeleted => {
//                         yield OperationOut {
//                             correlation,
//                             reply: Reply::StreamRead(ReadStreamResponse::StreamDeleted),
//                         };

//                         local_storage.complete(&correlation).await;
//                         return;
//                     }

//                     ReadStreamCompleted::Unexpected(e) => {
//                         unexpected_error(e)?;
//                         return;
//                     }

//                     ReadStreamCompleted::Success(streaming) => streaming,
//                 };

//                 let token = local_storage.new_cancellation_token(correlation).await;
//                 loop {
//                     select! {
//                         outcome = stream.next() => {
//                             match outcome {
//                                 Err(e) => {
//                                     yield OperationOut {
//                                         correlation,
//                                         reply: Reply::StreamRead(ReadStreamResponse::Unexpected(e)),
//                                     };

//                                    local_storage.complete(&correlation).await;
//                                    break;
//                                 }

//                                 Ok(entry) => {
//                                     match entry {
//                                         None => {
//                                         yield OperationOut {
//                                                 correlation,
//                                                 reply: Reply::StreamRead(ReadStreamResponse::EndOfStream),
//                                             };

//                                             local_storage.complete(&correlation).await;
//                                             break;
//                                         }

//                                         Some(entry) => {
//                                             yield OperationOut {
//                                                 correlation,
//                                                 reply: Reply::StreamRead(ReadStreamResponse::EventAppeared(entry)),
//                                             };
//                                         }
//                                     }
//                                 }
//                             }
//                         }

//                         _ = token.notified() => break,
//                     }
//                 }
//             }

//             Operation::Subscribe(subscribe) => {
//                 let token = local_storage.new_cancellation_token(correlation).await;
//                 match subscribe {
//                     Subscribe::ToStream(params) => {
//                         let mut catchup = if let Some(catchup) = CatchupSubscription::init(&internal, correlation, params).await? {
//                             catchup
//                         } else {
//                             yield OperationOut {
//                                 correlation,
//                                 reply: Reply::StreamRead(ReadStreamResponse::StreamDeleted),
//                             };

//                             local_storage.complete(&correlation).await;
//                             return;
//                         };

//                         yield OperationOut {
//                             correlation,
//                             reply: Reply::SubscriptionEvent(SubscriptionEvent::Confirmed(SubscriptionConfirmation::StreamName(catchup.params.stream_name.to_string()))),
//                         };

//                         loop {
//                             select! {
//                                 outcome = catchup.next() => {
//                                     match outcome {
//                                         Err(e) => {
//                                             tracing::error!(error = %e, stream = catchup.params.stream_name, "unexpected error when running catchup subscription");

//                                             yield OperationOut {
//                                                 correlation,
//                                                 reply: Reply::SubscriptionEvent(SubscriptionEvent::Unsubscribed(UnsubscribeReason::Server)),
//                                             };

//                                             break;
//                                         }

//                                         Ok(out) => {
//                                             if let Some(out) = out {
//                                                 yield out;
//                                                 continue;
//                                             } else {
//                                                 break;
//                                             }
//                                         }
//                                     }
//                                 }

//                                 _ = token.notified() => {
//                                     yield OperationOut {
//                                         correlation,
//                                         reply: Reply::SubscriptionEvent(SubscriptionEvent::Unsubscribed(UnsubscribeReason::User)),
//                                     };

//                                     break;
//                                 }
//                             }
//                         }

//                         local_storage.complete(&correlation).await;
//                     }

//                     Subscribe::ToProgram(params) => {
//                         let mut stream = internal.sub.subscribe_to_program(&params.name, &params.source).await?;

//                         yield OperationOut {
//                             correlation,
//                             reply: Reply::SubscriptionEvent(SubscriptionEvent::Confirmed(SubscriptionConfirmation::ProcessId(stream.id()))),
//                         };

//                         loop {
//                             select! {
//                                 outcome = stream.next() => {
//                                     match outcome {
//                                         Err(e) => {
//                                             tracing::error!(error = %e, name = params.name, proc_id = stream.id(), "unexpected error when running program");

//                                             yield OperationOut {
//                                                 correlation,
//                                                 reply: Reply::SubscriptionEvent(SubscriptionEvent::Unsubscribed(UnsubscribeReason::Server)),
//                                             };

//                                             break;
//                                         }

//                                         Ok(event) => {
//                                             if let Some(event) = event {
//                                                 yield OperationOut {
//                                                     correlation,
//                                                     reply: Reply::SubscriptionEvent(SubscriptionEvent::EventAppeared(event)),
//                                                 };
//                                                 continue;
//                                             } else {
//                                                 break;
//                                             }
//                                         }
//                                     }
//                                 }

//                                 _ = token.notified() => {
//                                     yield OperationOut {
//                                         correlation,
//                                         reply: Reply::SubscriptionEvent(SubscriptionEvent::Unsubscribed(UnsubscribeReason::User)),
//                                     };

//                                     break;
//                                 }
//                             }
//                         }

//                         local_storage.complete(&correlation).await;
//                     }
//                 };
//             }

//             Operation::ListPrograms(_) => {
//                 let programs = internal.sub.list_programs().await?;
//                 yield OperationOut {
//                     correlation,
//                     reply: Reply::ProgramsListed(ProgramListed { programs }),
//                 };
//             }

//             Operation::GetProgramStats(params) => {
//                 let result = match internal.sub.program_stats(params.id).await? {
//                     Some(stats) => ProgramObtained::Success(stats),
//                     None => ProgramObtained::Error(GetProgramError::NotExists),
//                 };

//                 yield OperationOut {
//                     correlation,
//                     reply: Reply::ProgramObtained(result),
//                 };
//             }

//             Operation::KillProgram(params) => {
//                 internal.sub.program_stop(params.id).await?;
//                 yield OperationOut {
//                     correlation,
//                     reply: Reply::ProgramKilled(ProgramKilled::Success),
//                 };
//             }

//             Operation::Unsubscribe => {
//                 local_storage.cancel(&correlation).await;
//             }
//         };
//     }
// }

struct CatchupSubscription {
    params: SubscribeToStream,
    catching_up: bool,
    history: VecDeque<Record>,
    end_revision: u64,
    done: bool,
    read_stream: crate::process::reading::Streaming,
    sub_stream: crate::process::subscription::Streaming,
}

impl CatchupSubscription {
    async fn init(
        internal: &ProtocolImpl,
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
            catching_up: false,
            history: VecDeque::new(),
            done: false,
            end_revision,
            read_stream,
            sub_stream,
        }))
    }

    // CAUTION: a situation where an user is reading very far away from the head of the stream and while that stream is actively being writen on could lead
    // to uncheck memory usage as everything will be stored in the history buffer.
    //
    // TODO: Implement a mechanism to limit the size of the history buffer by implementing a backpressure mechanism.
    async fn next(&mut self) -> eyre::Result<Option<SubscriptionEvent>> {
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
                                return Ok(Some(SubscriptionEvent::EventAppeared(event)));
                            } else {
                                self.catching_up = false;
                                return Ok(Some(SubscriptionEvent::CaughtUp));
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
                                return Ok(None);
                            }
                        }
                    }
                }
            }
        }

        if let Some(event) = self.history.pop_front() {
            return Ok(Some(SubscriptionEvent::EventAppeared(event)));
        }

        if let Some(event) = self.sub_stream.next().await? {
            return Ok(Some(SubscriptionEvent::EventAppeared(event)));
        }

        self.done = true;

        Ok(None)
    }
}
