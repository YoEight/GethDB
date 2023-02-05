use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use geth_common::protocol::streams::{
    append_req, append_resp, read_resp, server::Streams, AppendReq, AppendResp, BatchAppendReq,
    BatchAppendResp, CountOption, DeleteReq, DeleteResp, ReadEvent, ReadReq, ReadResp,
    RecordedEvent, StreamOption, Success, TombstoneReq, TombstoneResp,
};

use crate::bus::Bus;
use crate::messages::{AppendStream, ReadStream};
use futures::stream::BoxStream;
use futures::TryStreamExt;
use geth_common::{Direction, ExpectedRevision, Propose};
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
        let req = request.into_inner();

        let options = req
            .options
            .ok_or(Status::invalid_argument("Options was not provided"))?;

        let stream_name = options
            .stream_option
            .ok_or(Status::invalid_argument("Stream name argument is missing"))?;

        let (stream_name, revision) = match stream_name {
            StreamOption::Stream(options) => {
                let ident = if let Some(ident) = options.stream_identifier {
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

        let count = match count {
            CountOption::Count(c) => c as usize,
            CountOption::Subscription(_) => {
                return Err(Status::unimplemented(
                    "Subscriptions are not implemented yet",
                ));
            }
        };

        let msg = ReadStream {
            correlation: Uuid::new_v4(),
            starting: revision,
            count,
            stream_name,
            direction,
        };

        let mut reader = self.bus.read_stream(msg).await.reader;
        let streaming = async_stream::stream! {
            loop {
                match reader.next().await {
                    Err(e) => {
                        yield Err(Status::unavailable(e.to_string()));
                        break;
                    }
                    Ok(record) => {
                        if let Some(record) = record {
                            let raw_pos = record.position.raw();
                            let event = RecordedEvent {
                                id: Some(record.id.into()),
                                stream_identifier: Some(record.stream.into()),
                                stream_revision: record.revision,
                                prepare_position: raw_pos,
                                commit_position: raw_pos,
                                metadata: Default::default(),
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

        let resp = self
            .bus
            .append_stream(AppendStream {
                correlation: Uuid::new_v4(),
                stream_name,
                events,
                expected,
            })
            .await;

        let resp = AppendResp {
            result: Some(append_resp::Result::Success(Success {
                current_revision_option: Some(resp.next_revision.into()),
                position_option: Some(append_resp::success::PositionOption::Position(
                    resp.result.position.into(),
                )),
            })),
        };

        Ok(Response::new(resp))
    }

    async fn delete(&self, _request: Request<DeleteReq>) -> Result<Response<DeleteResp>, Status> {
        todo!()
    }

    async fn tombstone(
        &self,
        _request: Request<TombstoneReq>,
    ) -> Result<Response<TombstoneResp>, Status> {
        todo!()
    }

    type BatchAppendStream = BoxStream<'static, Result<BatchAppendResp, Status>>;

    async fn batch_append(
        &self,
        _request: Request<Streaming<BatchAppendReq>>,
    ) -> Result<Response<Self::BatchAppendStream>, Status> {
        todo!()
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

        return Some(Propose { id, data });
    }

    None
}