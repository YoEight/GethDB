use byteorder::BigEndian;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use super::generated::geth::protocol::{
    self,
    server::{
        append_req::{self, ProposedMessage},
        append_resp::{self, Success},
        read_resp::{
            self,
            read_event::{self, RecordedEvent},
            ReadEvent,
        },
        streams_server::Streams,
        AppendReq, AppendResp, BatchAppendReq, BatchAppendResp, DeleteReq, DeleteResp, ReadReq,
        ReadResp, TombstoneReq, TombstoneResp,
    },
    StreamIdentifier,
};

use crate::bus::Bus;
use crate::grpc::generated::geth::protocol::server::read_req::options::stream_options::RevisionOption;
use crate::grpc::generated::geth::protocol::server::read_req::options::{
    CountOption, StreamOption,
};
use crate::messages::{AppendStream, ReadStream};
use crate::types::{Direction, ExpectedRevision, Propose, Revision};
use byteorder::ByteOrder;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use uuid::Uuid;

fn uuid_to_grpc(value: Uuid) -> protocol::Uuid {
    let buf = value.as_bytes();

    let most_significant_bits = BigEndian::read_i64(&buf[0..8]);
    let least_significant_bits = BigEndian::read_i64(&buf[8..16]);

    protocol::Uuid {
        value: Some(protocol::uuid::Value::Structured(
            protocol::uuid::Structured {
                most_significant_bits,
                least_significant_bits,
            },
        )),
    }
}

fn grpc_to_uuid(value: protocol::Uuid) -> Option<Uuid> {
    match value.value? {
        protocol::uuid::Value::Structured(s) => {
            let mut buf = [0u8; 16];

            BigEndian::write_i64(&mut buf, s.most_significant_bits);
            BigEndian::write_i64(&mut buf[8..16], s.least_significant_bits);

            Some(Uuid::from_bytes(buf))
        }

        protocol::uuid::Value::String(s) => Uuid::try_from(s.as_str()).ok(),
    }
}

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
                    String::from_utf8(ident.stream_name)
                        .map_err(|_| Status::invalid_argument("Stream name is not valid UTF-8"))?
                } else {
                    return Err(Status::invalid_argument("Stream name argument is missing"));
                };

                let revision = options
                    .revision_option
                    .ok_or(Status::invalid_argument("revision option is not provided"))?;

                let revision = match revision {
                    RevisionOption::Revision(rev) => Revision::Revision(rev),
                    RevisionOption::Start(_) => Revision::Start,
                    RevisionOption::End(_) => Revision::End,
                };

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
                                id: Some(uuid_to_grpc(record.id)),
                                stream_identifier: Some(StreamIdentifier {
                                    stream_name: record.stream.into_bytes(),
                                }),
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
                                position: Some(read_event::Position::CommitPosition(raw_pos)),
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

        let commit_position = resp.result.position.raw();
        let prepare_position = commit_position;
        let next_revision = if let Some(revision) = resp.next_revision {
            append_resp::success::CurrentRevisionOption::CurrentRevision(revision)
        } else {
            append_resp::success::CurrentRevisionOption::NoStream(protocol::Empty::default())
        };

        let resp = AppendResp {
            result: Some(append_resp::Result::Success(Success {
                current_revision_option: Some(next_revision),
                position_option: Some(append_resp::success::PositionOption::Position(
                    append_resp::Position {
                        commit_position,
                        prepare_position,
                    },
                )),
            })),
        };

        Ok(Response::new(resp))
    }

    async fn delete(&self, request: Request<DeleteReq>) -> Result<Response<DeleteResp>, Status> {
        todo!()
    }

    async fn tombstone(
        &self,
        request: Request<TombstoneReq>,
    ) -> Result<Response<TombstoneResp>, Status> {
        todo!()
    }

    type BatchAppendStream = BoxStream<'static, Result<BatchAppendResp, Status>>;

    async fn batch_append(
        &self,
        request: Request<Streaming<BatchAppendReq>>,
    ) -> Result<Response<Self::BatchAppendStream>, Status> {
        todo!()
    }
}

fn parse_append_options(req: AppendReq) -> Option<(String, ExpectedRevision)> {
    if let append_req::Content::Options(options) = req.content? {
        let expected_revision = match options.expected_stream_revision? {
            append_req::options::ExpectedStreamRevision::Revision(v) => {
                ExpectedRevision::Revision(v)
            }

            append_req::options::ExpectedStreamRevision::NoStream(_) => ExpectedRevision::NoStream,
            append_req::options::ExpectedStreamRevision::StreamExists(_) => {
                ExpectedRevision::StreamsExists
            }
            append_req::options::ExpectedStreamRevision::Any(_) => ExpectedRevision::Any,
        };

        let ident = options.stream_identifier?;
        let stream_name = String::from_utf8(ident.stream_name).ok()?;

        return Some((stream_name, expected_revision));
    }

    None
}

fn parse_propose(req: AppendReq) -> Option<Propose> {
    if let append_req::Content::ProposedMessage(msg) = req.content? {
        let id = msg.id.and_then(grpc_to_uuid)?;
        let data = msg.data.into();

        return Some(Propose { id, data });
    }

    None
}
