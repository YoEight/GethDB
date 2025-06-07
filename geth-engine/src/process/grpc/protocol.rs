use std::collections::VecDeque;

use geth_common::protocol::protocol_server::Protocol;
use geth_common::protocol::{self, SubscribeResponse};
use geth_mikoshi::hashing::mikoshi_hash;
use tokio::select;
use tokio::sync::mpsc::unbounded_channel;
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;

use geth_common::{
    AppendStream, DeleteStream, Direction, GetProgramStats, KillProgram, ProgramKilled,
    ProgramListed, ProgramObtained, ReadStream, ReadStreamCompleted, ReadStreamResponse, Record,
    Subscribe, SubscribeToStream, SubscriptionEvent, UnsubscribeReason,
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
        request: Request<protocol::ProgramStatsRequest>,
    ) -> Result<Response<protocol::ProgramStatsResponse>, Status> {
        let params: GetProgramStats = request.into_inner().into();
        match self.sub.program_stats(params.id).await {
            Err(e) => Err(Status::internal(e.to_string())),
            Ok(stats) => {
                if let Some(stats) = stats {
                    Ok(Response::new(ProgramObtained::Success(stats).into()))
                } else {
                    Err(Status::not_found("program-not-found"))
                }
            }
        }
    }

    async fn stop_program(
        &self,
        request: Request<protocol::StopProgramRequest>,
    ) -> Result<Response<protocol::StopProgramResponse>, Status> {
        let params: KillProgram = request.into_inner().into();
        if let Err(e) = self.sub.program_stop(params.id).await {
            return Err(Status::internal(e.to_string()));
        }

        Ok(Response::new(ProgramKilled::Success.into()))
    }
}

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
