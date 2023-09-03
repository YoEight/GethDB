use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use geth_common::protocol::{
    self,
    streams::{
        append_req, append_resp, delete_resp, list_progs_resp::ProgrammableSubscriptionSummary,
        read_resp, server::Streams, AppendReq, AppendResp, CountOption, DeleteReq, DeleteResp,
        KillProgReq, KillProgResp, ListProgsResp, ProgStatsReq, ProgStatsResp, ReadEvent, ReadReq,
        ReadResp, RecordedEvent, StreamOption, Success,
    },
    Empty,
};

use crate::bus::Bus;
use crate::messages::{
    AppendStream, DeleteStream, DeleteStreamCompleted, ProcessTarget, ReadStream,
    ReadStreamCompleted, StreamTarget, SubscriptionRequestOutcome, SubscriptionTarget,
};
use crate::messages::{AppendStreamCompleted, SubscribeTo};
use futures::stream::BoxStream;
use futures::TryStreamExt;
use geth_common::protocol::streams::read_req::options::subscription_options::SubKind;
use geth_common::{Direction, ExpectedRevision, Propose};
use prost_types::Timestamp;
use std::collections::HashMap;
use uuid::Uuid;

pub struct StreamsImpl {
    bus: Bus,
}

impl StreamsImpl {
    pub fn new(bus: Bus) -> Self {
        Self { bus }
    }
}

#[tonic::async_trait]
impl Streams for StreamsImpl {
    type ReadStream = BoxStream<'static, Result<ReadResp, Status>>;
    async fn read(&self, request: Request<ReadReq>) -> Result<Response<Self::ReadStream>, Status> {
        tracing::info!("ReadStream request received!");
        let req = request.into_inner();

        let options = req
            .options
            .ok_or(Status::invalid_argument("Options was not provided"))?;

        let stream_name = options
            .stream_option
            .ok_or(Status::invalid_argument("Stream name argument is missing"))?;

        let (stream_name, revision) = match stream_name {
            StreamOption::Stream(options) => {
                let ident: String = if let Some(ident) = options.stream_identifier {
                    ident
                        .try_into()
                        .map_err(|_| Status::invalid_argument("Stream name is not valid UTF-8"))?
                } else {
                    return Err(Status::invalid_argument("Stream name argument is missing"));
                };

                let revision = options
                    .revision_option
                    .ok_or(Status::invalid_argument("revision option is not provided"))?
                    .into();

                (ident, revision)
            }

            StreamOption::All(_) => {
                return Err(Status::unimplemented(
                    "Reading from $all is not implemented yet",
                ));
            }
        };

        let direction = Direction::try_from(options.read_direction)
            .map_err(|_| Status::invalid_argument("Wrong direction value"))?;

        let count = options
            .count_option
            .ok_or(Status::invalid_argument("count_options was not provided"))?;

        let mut reader = match count {
            CountOption::Count(count) => {
                let msg = ReadStream {
                    correlation: Uuid::new_v4(),
                    starting: revision,
                    count: count as usize,
                    stream_name: stream_name.clone(),
                    direction,
                };

                tracing::info!("Managed to parse read stream successfully");
                match self.bus.read_stream(msg).await {
                    Ok(resp) => match resp {
                        ReadStreamCompleted::StreamDeleted => {
                            return Err(Status::not_found(format!(
                                "Stream '{}' is deleted",
                                stream_name
                            )))
                        }
                        ReadStreamCompleted::Unexpected(e) => {
                            return Err(Status::unavailable(format!("Unexpected error: {}", e)))
                        }
                        ReadStreamCompleted::Success(reader) => reader,
                    },
                    Err(e) => {
                        tracing::error!("Error when reading from mikoshi: {}", e);
                        return Err(Status::unavailable(e.to_string()));
                    }
                }
            }

            CountOption::Subscription(opts) => {
                let target = match opts.sub_kind.unwrap() {
                    SubKind::Regular(_) => SubscriptionTarget::Stream(StreamTarget {
                        parent: None,
                        stream_name,
                        starting: revision,
                    }),

                    SubKind::Programmable(opts) => SubscriptionTarget::Process(ProcessTarget {
                        id: Uuid::new_v4(),
                        name: opts.name,
                        source_code: opts.source_code,
                    }),
                };

                let msg = SubscribeTo {
                    correlation: Uuid::new_v4(),
                    target,
                };

                match self.bus.subscribe_to(msg).await {
                    Ok(resp) => match resp.outcome {
                        SubscriptionRequestOutcome::Success(reader) => reader,
                        SubscriptionRequestOutcome::Failure(e) => {
                            return Err(Status::aborted(e.to_string()));
                        }
                    },
                    Err(e) => {
                        tracing::error!("Error when subscribing: {}", e);
                        return Err(Status::unavailable(e.to_string()));
                    }
                }
            }
        };

        let streaming = async_stream::stream! {
            loop {
                match reader.next().await {
                    Err(e) => {
                        tracing::error!("Error when streaming from mikoshi: {}", e);
                        yield Err(Status::unavailable(e.to_string()));
                        break;
                    }
                    Ok(record) => {
                        tracing::info!("Record received: {:?}", record);

                        if let Some(record) = record {
                            let mut metadata = HashMap::new();
                            metadata.insert("type".to_string(), record.r#type);

                            let raw_pos = record.position.raw();
                            let event = RecordedEvent {
                                id: Some(record.id.into()),
                                stream_identifier: Some(record.stream_name.into()),
                                stream_revision: record.revision,
                                prepare_position: raw_pos,
                                commit_position: raw_pos,
                                metadata,
                                custom_metadata: Default::default(),
                                data: record.data.to_vec(),
                            };

                            let event = ReadEvent {
                                event: Some(event),
                                link: None,
                                position: Some(record.position.into()),
                            };

                            yield Ok(ReadResp {
                                content: Some(read_resp::Content::Event(event)),
                            });

                            continue;
                        }

                        break;
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(streaming)))
    }

    async fn append(
        &self,
        request: Request<Streaming<AppendReq>>,
    ) -> Result<Response<AppendResp>, Status> {
        tracing::info!("AppendStream request received!");
        let mut reader = request.into_inner();
        let mut events = Vec::new();

        let (stream_name, expected) =
            if let Some(options) = reader.try_next().await?.and_then(parse_append_options) {
                options
            } else {
                return Err(Status::invalid_argument(
                    "No stream name or expected version was provided",
                ));
            };

        while let Some(event) = reader.try_next().await?.and_then(parse_propose) {
            events.push(event);
        }

        tracing::info!("Manage to parse append stream request");
        let resp = self
            .bus
            .append_stream(AppendStream {
                correlation: Uuid::new_v4(),
                stream_name: stream_name.clone(),
                events,
                expected,
            })
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;

        let resp = match resp {
            AppendStreamCompleted::Success(result) => AppendResp {
                result: Some(append_resp::Result::Success(Success {
                    current_revision_option: Some(result.next_expected_version.into()),
                    position_option: Some(append_resp::success::PositionOption::Position(
                        result.position.into(),
                    )),
                })),
            },

            AppendStreamCompleted::Failure(e) => AppendResp {
                result: Some(append_resp::Result::WrongExpectedVersion(
                    append_resp::WrongExpectedVersion {
                        current_revision_option_20_6_0: None,
                        expected_revision_option_20_6_0: None,
                        current_revision_option: Some(e.current.into()),
                        expected_revision_option: Some(e.expected.into()),
                    },
                )),
            },
            AppendStreamCompleted::Unexpected(e) => {
                return Err(Status::unavailable(format!("Unexpected error: {}", e)))
            }
            AppendStreamCompleted::StreamDeleted => {
                return Err(Status::not_found(format!(
                    "Stream '{}' is deleted",
                    stream_name
                )))
            }
        };

        Ok(Response::new(resp))
    }

    async fn delete(&self, request: Request<DeleteReq>) -> Result<Response<DeleteResp>, Status> {
        let request = request.into_inner();
        let options = request
            .options
            .ok_or(Status::invalid_argument("Options field is missing"))?;

        let stream_name = match options
            .stream_identifier
            .ok_or(Status::invalid_argument("stream_name field is missing"))?
            .try_into()
        {
            Err(e) => {
                return Err(Status::invalid_argument(format!(
                    "stream_name field is not a valid utf-8 format: {}",
                    e
                )))
            }
            Ok(s) => s,
        };

        let expected = options
            .expected_stream_revision
            .ok_or(Status::invalid_argument(
                "expected_stream_revision field is missing",
            ))?
            .into();

        match self
            .bus
            .delete_stream(DeleteStream {
                stream_name,
                expected,
            })
            .await
        {
            Err(e) => Err(Status::unavailable(format!(
                "Error when deleting stream: {}",
                e
            ))),

            Ok(result) => {
                let resp = match result {
                    DeleteStreamCompleted::Success(result) => DeleteResp {
                        result: Some(delete_resp::Result::Position(delete_resp::Position {
                            commit_position: result.position.raw(),
                            prepare_position: result.position.raw(),
                        })),
                    },

                    DeleteStreamCompleted::Failure(e) => DeleteResp {
                        result: Some(delete_resp::Result::WrongExpectedVersion(
                            delete_resp::WrongExpectedVersion {
                                current_revision_option: Some(e.current.into()),
                                expected_revision_option: Some(e.expected.into()),
                            },
                        )),
                    },

                    DeleteStreamCompleted::Unexpected(e) => {
                        return Err(Status::unavailable(format!("Unexpected error: {}", e)));
                    }
                };

                Ok(Response::new(resp))
            }
        }
    }

    async fn get_programmable_subscription_stats(
        &self,
        request: Request<ProgStatsReq>,
    ) -> Result<Response<ProgStatsResp>, Status> {
        let id = parse_required_uuid("id", request.into_inner().id)?;

        match self.bus.get_programmable_subscription_stats(id).await {
            Err(_) => Err(Status::unavailable("Server is down")),
            Ok(stats) => {
                if let Some(stats) = stats {
                    let mut started = Timestamp::default();
                    started.seconds = stats.started.timestamp();

                    Ok(Response::new(ProgStatsResp {
                        id: Some(id.into()),
                        name: stats.name,
                        source_code: stats.source_code,
                        pushed_events: stats.pushed_events as u64,
                        started: Some(started),
                        subscriptions: stats.subscriptions,
                    }))
                } else {
                    Err(Status::not_found(format!(
                        "Programmable subscription with id '{}' is not found",
                        id
                    )))
                }
            }
        }
    }

    async fn kill_programmable_subscription(
        &self,
        request: Request<KillProgReq>,
    ) -> Result<Response<KillProgResp>, Status> {
        let id = parse_required_uuid("id", request.into_inner().id)?;

        if let Err(_) = self.bus.kill_programmable_subscription(id).await {
            Err(Status::unavailable("Server is down"))
        } else {
            Ok(Response::new(KillProgResp {
                id: Some(id.into()),
            }))
        }
    }

    async fn list_programmable_subscriptions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ListProgsResp>, Status> {
        match self.bus.list_programmable_subscriptions().await {
            Err(_) => Err(Status::unavailable("Server is down")),
            Ok(progs) => Ok(Response::new(ListProgsResp {
                summaries: progs
                    .into_iter()
                    .map(|s| {
                        let mut started = Timestamp::default();
                        started.seconds = s.started.timestamp();

                        ProgrammableSubscriptionSummary {
                            id: Some(s.id.into()),
                            name: s.name.clone(),
                            started: Some(started),
                        }
                    })
                    .collect(),
            })),
        }
    }
}

fn parse_append_options(req: AppendReq) -> Option<(String, ExpectedRevision)> {
    if let append_req::Content::Options(options) = req.content? {
        let expected_revision = options.expected_stream_revision?.into();
        let ident = options.stream_identifier?;
        let stream_name = ident.try_into().ok()?;

        return Some((stream_name, expected_revision));
    }

    None
}

fn parse_propose(req: AppendReq) -> Option<Propose> {
    if let append_req::Content::ProposedMessage(msg) = req.content? {
        let id = msg.id.and_then(|x| x.try_into().ok())?;
        let data = msg.data.into();
        let r#type = msg.metadata.get("type").cloned().unwrap_or_default();

        return Some(Propose { id, data, r#type });
    }

    None
}

fn parse_required_uuid(field_name: &str, grpc_uuid: Option<protocol::Uuid>) -> tonic::Result<Uuid> {
    if let Some(grpc_uuid) = grpc_uuid {
        parse_uuid(grpc_uuid)
    } else {
        return Err(Status::invalid_argument(format!(
            "{} must be defined",
            field_name
        )));
    }
}

fn parse_uuid(grpc_uuid: protocol::Uuid) -> tonic::Result<Uuid> {
    match grpc_uuid.try_into() {
        Ok(id) => Ok(id),
        Err(e) => {
            return Err(Status::invalid_argument(format!(
                "id is an invalid UUID: {}",
                e
            )))
        }
    }
}
