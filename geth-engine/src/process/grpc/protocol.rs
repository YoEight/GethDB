use geth_common::protocol::protocol_server::Protocol;
use geth_common::protocol::{self, SubscribeResponse};
use tokio::sync::mpsc::unbounded_channel;
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;

use geth_common::{
    AppendStream, DeleteStream, GetProgramStats, KillProgram, ProgramKilled, ProgramListed,
    ProgramObtained, ReadStream, ReadStreamCompleted, ReadStreamResponse, Subscribe,
    SubscriptionEvent, UnsubscribeReason,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::process::consumer::{start_consumer, ConsumerResult};
use crate::process::reading::ReaderClient;
use crate::process::subscription::SubscriptionClient;
use crate::process::writing::WriterClient;
use crate::process::{ManagerClient, RequestContext};

#[derive(Clone)]
pub struct ProtocolImpl {
    writer: WriterClient,
    reader: ReaderClient,
    sub: SubscriptionClient,
}

#[allow(clippy::result_large_err)]
pub fn try_get_request_context_from<A>(req: &Request<A>) -> Result<RequestContext, tonic::Status> {
    let metadata = req.metadata();
    if let Some(correlation) = metadata.get("correlation") {
        let correlation = correlation.to_str().map_err(|e| {
            tonic::Status::invalid_argument(format!("invalid correlation metadata value: {}", e))
        })?;

        let correlation = Uuid::parse_str(correlation).map_err(|e| {
            tonic::Status::invalid_argument(format!("invalid correlation UUID value: {}", e))
        })?;

        return Ok(RequestContext { correlation });
    }

    Ok(RequestContext::new())
}

impl ProtocolImpl {
    pub async fn connect(client: ManagerClient) -> eyre::Result<Self> {
        Ok(Self {
            writer: client.new_writer_client().await?,
            reader: client.new_reader_client().await?,
            sub: client.new_subscription_client().await?,
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
        let ctx = try_get_request_context_from(&request)?;
        let params: AppendStream = request.into_inner().into();
        match self
            .writer
            .append(
                ctx,
                params.stream_name,
                params.expected_revision,
                params.events,
            )
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
        let ctx = try_get_request_context_from(&request)?;
        let params: ReadStream = request.into_inner().into();

        match self
            .reader
            .read(
                ctx,
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
        let ctx = try_get_request_context_from(&request)?;
        let params: DeleteStream = request.into_inner().into();

        match self
            .writer
            .delete(ctx, params.stream_name, params.expected_revision)
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
        let ctx = try_get_request_context_from(&request)?;
        let (sender, recv) = unbounded_channel::<Result<SubscribeResponse, Status>>();

        match request.into_inner().into() {
            Subscribe::ToStream(params) => {
                let mut consumer = match start_consumer(
                    ctx,
                    params.stream_name.clone(),
                    params.start,
                    self.reader.manager(),
                )
                .await
                {
                    Err(e) => return Err(Status::internal(e.to_string())),
                    Ok(result) => match result {
                        ConsumerResult::Success(c) => c,
                        ConsumerResult::StreamDeleted => {
                            return Err(Status::failed_precondition("stream-deleted"))
                        }
                    },
                };

                tokio::spawn(async move {
                    loop {
                        match consumer.next().await {
                            Err(e) => {
                                let _ = sender.send(Err(Status::internal(e.to_string())));

                                break;
                            }

                            Ok(event) => {
                                if let Some(event) = event {
                                    if sender.send(Ok(event.into())).is_err() {
                                        tracing::debug!(
                                            stream = params.stream_name,
                                            "user disconnected from catchup subscription"
                                        );

                                        break;
                                    }
                                } else {
                                    tracing::debug!(
                                        stream = params.stream_name,
                                        "server ended catchup subscription"
                                    );

                                    let _ = sender.send(Ok(SubscriptionEvent::Unsubscribed(
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

            Subscribe::ToProgram(params) => {
                match self
                    .sub
                    .subscribe_to_program(ctx, &params.name, &params.source)
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
                                            if sender.send(Ok(event.into())).is_err() {
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
        request: Request<protocol::ListProgramsRequest>,
    ) -> Result<Response<protocol::ListProgramsResponse>, Status> {
        let ctx = try_get_request_context_from(&request)?;
        match self.sub.list_programs(ctx).await {
            Err(e) => Err(Status::internal(e.to_string())),
            Ok(programs) => Ok(Response::new(ProgramListed { programs }.into())),
        }
    }

    async fn program_stats(
        &self,
        request: Request<protocol::ProgramStatsRequest>,
    ) -> Result<Response<protocol::ProgramStatsResponse>, Status> {
        let ctx = try_get_request_context_from(&request)?;
        let params: GetProgramStats = request.into_inner().into();
        match self.sub.program_stats(ctx, params.id).await {
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
        let ctx = try_get_request_context_from(&request)?;
        let params: KillProgram = request.into_inner().into();
        if let Err(e) = self.sub.program_stop(ctx, params.id).await {
            return Err(Status::internal(e.to_string()));
        }

        Ok(Response::new(ProgramKilled::Success.into()))
    }
}
