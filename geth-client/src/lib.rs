use std::collections::HashMap;

use chrono::{TimeZone, Utc};
use futures_util::TryStreamExt;
use tonic::{
    codegen::StdError,
    Request,
    Streaming, transport::{self, Channel, Endpoint},
};
use uuid::Uuid;

use geth_common::{
    DeleteResult,
    Direction, ExpectedRevision, Position, ProgrammableStats, ProgrammableSummary, Propose,
    protocol::streams::{
        append_req::{self, ProposedMessage},
        append_resp,
        AppendReq,
        client::StreamsClient,
        read_req::{self, options::StreamOptions}, read_resp, ReadReq, ReadResp,
    }, Record, Revision, WriteResult, WrongExpectedRevisionError,
};
use geth_common::protocol::Empty;
use geth_common::protocol::streams::{
    delete_req, delete_resp, DeleteReq, KillProgReq, ProgStatsReq,
};
use geth_common::protocol::streams::read_req::options::{Programmable, SubscriptionOptions};
use geth_common::protocol::streams::read_req::options::subscription_options::SubKind;

mod next;
mod types;

#[derive(Clone)]
pub struct Client {
    inner: StreamsClient<Channel>,
}

impl Client {
    pub async fn new<D>(dest: D) -> Result<Self, transport::Error>
    where
        D: TryInto<Endpoint>,
        D::Error: Into<StdError>,
    {
        let inner = StreamsClient::connect(dest).await?;

        Ok(Self { inner })
    }

    pub async fn append_stream(
        &mut self,
        stream_name: impl AsRef<str>,
        events: Vec<Propose>,
        expected: ExpectedRevision,
    ) -> eyre::Result<WriteResult> {
        let mut msgs = Vec::new();

        msgs.push(AppendReq {
            content: Some(append_req::Content::Options(append_req::Options {
                stream_identifier: Some(stream_name.as_ref().into()),
                expected_stream_revision: Some(expected.into()),
            })),
        });

        for p in events {
            let mut metadata = HashMap::new();

            metadata.insert("type".to_string(), p.r#type);
            metadata.insert("content-type".to_string(), "application/json".to_string());

            msgs.push(AppendReq {
                content: Some(append_req::Content::ProposedMessage(ProposedMessage {
                    id: Some(p.id.into()),
                    metadata,
                    custom_metadata: Default::default(),
                    data: p.data.to_vec(),
                })),
            });
        }

        let resp = self
            .inner
            .append(futures_util::stream::iter(msgs))
            .await?
            .into_inner();

        match resp.result.expect("to be defined") {
            append_resp::Result::Success(s) => {
                let next_expected_version = s.current_revision_option.unwrap().into();
                let position = s.position_option.unwrap().into();

                Ok(WriteResult {
                    next_expected_version,
                    position,
                    next_logical_position: 0,
                })
            }
            append_resp::Result::WrongExpectedVersion(err) => {
                let expected = err.expected_revision_option.unwrap().into();
                let current = err.current_revision_option.unwrap().into();

                Err(WrongExpectedRevisionError { expected, current }.into())
            }
        }
    }

    pub async fn read_stream(
        &mut self,
        stream_name: impl AsRef<str>,
        start: Revision<u64>,
        direction: Direction,
    ) -> eyre::Result<ReadStream> {
        let stream = self
            .inner
            .read(Request::new(ReadReq {
                options: Some(read_req::Options {
                    read_direction: direction.into(),
                    resolve_links: false,
                    uuid_option: Some(read_req::options::UuidOption {
                        content: Some(read_req::options::uuid_option::Content::Structured(
                            Default::default(),
                        )),
                    }),
                    control_option: Some(read_req::options::ControlOption { compatibility: 1 }),
                    stream_option: Some(read_req::options::StreamOption::Stream(StreamOptions {
                        stream_identifier: Some(stream_name.as_ref().into()),
                        revision_option: Some(start.into()),
                    })),
                    count_option: Some(read_req::options::CountOption::Count(u64::MAX)),
                    filter_option: Some(read_req::options::FilterOption::NoFilter(
                        Default::default(),
                    )),
                }),
            }))
            .await?
            .into_inner();

        Ok(ReadStream { inner: stream })
    }

    pub async fn subscribe_to_stream(
        &mut self,
        stream_name: impl AsRef<str>,
        start: Revision<u64>,
    ) -> tonic::Result<ReadStream> {
        let stream = self
            .inner
            .read(Request::new(ReadReq {
                options: Some(read_req::Options {
                    read_direction: Direction::Forward.into(),
                    resolve_links: false,
                    uuid_option: Some(read_req::options::UuidOption {
                        content: Some(read_req::options::uuid_option::Content::Structured(
                            Default::default(),
                        )),
                    }),
                    control_option: Some(read_req::options::ControlOption { compatibility: 1 }),
                    stream_option: Some(read_req::options::StreamOption::Stream(StreamOptions {
                        stream_identifier: Some(stream_name.as_ref().into()),
                        revision_option: Some(start.into()),
                    })),
                    count_option: Some(read_req::options::CountOption::Subscription(
                        SubscriptionOptions {
                            sub_kind: Some(SubKind::Regular(Empty {})),
                        },
                    )),
                    filter_option: Some(read_req::options::FilterOption::NoFilter(
                        Default::default(),
                    )),
                }),
            }))
            .await?
            .into_inner();

        Ok(ReadStream { inner: stream })
    }

    pub async fn subscribe_to_process(
        &mut self,
        name: impl AsRef<str>,
        source_code: impl AsRef<str>,
    ) -> tonic::Result<ReadStream> {
        let stream = self
            .inner
            .read(Request::new(ReadReq {
                options: Some(read_req::Options {
                    read_direction: Direction::Forward.into(),
                    resolve_links: false,
                    uuid_option: Some(read_req::options::UuidOption {
                        content: Some(read_req::options::uuid_option::Content::Structured(
                            Default::default(),
                        )),
                    }),
                    control_option: Some(read_req::options::ControlOption { compatibility: 1 }),
                    stream_option: Some(read_req::options::StreamOption::Stream(StreamOptions {
                        // TODO - This property will not be used, we will improve the API later.
                        stream_identifier: Some(name.as_ref().into()),
                        // TODO - This property will not be used, we will improve the API later.
                        revision_option: Some(Revision::Start.into()),
                    })),
                    count_option: Some(read_req::options::CountOption::Subscription(
                        SubscriptionOptions {
                            sub_kind: Some(SubKind::Programmable(Programmable {
                                name: name.as_ref().to_string(),
                                source_code: source_code.as_ref().to_string(),
                            })),
                        },
                    )),
                    filter_option: Some(read_req::options::FilterOption::NoFilter(
                        Default::default(),
                    )),
                }),
            }))
            .await?
            .into_inner();

        Ok(ReadStream { inner: stream })
    }

    pub async fn get_programmable_subscription_stats(
        &mut self,
        id: Uuid,
    ) -> tonic::Result<ProgrammableStats> {
        let stats = self
            .inner
            .get_programmable_subscription_stats(Request::new(ProgStatsReq {
                id: Some(id.into()),
            }))
            .await?
            .into_inner();

        let started = Utc
            .timestamp_opt(stats.started.unwrap().seconds, 0)
            .unwrap();

        Ok(ProgrammableStats {
            id: stats.id.unwrap().try_into().unwrap(),
            name: stats.name,
            source_code: stats.source_code,
            subscriptions: stats.subscriptions,
            pushed_events: stats.pushed_events as usize,
            started,
        })
    }

    pub async fn kill_programmable_subscription(&mut self, id: Uuid) -> tonic::Result<()> {
        self.inner
            .kill_programmable_subscription(Request::new(KillProgReq {
                id: Some(id.into()),
            }))
            .await?;

        Ok(())
    }

    pub async fn list_programmable_subscriptions(
        &mut self,
    ) -> tonic::Result<Vec<ProgrammableSummary>> {
        let summaries_grpc = self
            .inner
            .list_programmable_subscriptions(Request::new(Empty {}))
            .await?
            .into_inner()
            .summaries;

        let mut summaries = Vec::with_capacity(summaries_grpc.len());

        for summary in summaries_grpc {
            summaries.push(ProgrammableSummary {
                id: summary.id.unwrap().try_into().unwrap(),
                name: summary.name,
                started: Utc
                    .timestamp_opt(summary.started.unwrap().seconds, 0)
                    .unwrap(),
            });
        }

        Ok(summaries)
    }

    pub async fn delete_stream(
        &mut self,
        stream_name: impl AsRef<str>,
        expected: ExpectedRevision,
    ) -> tonic::Result<DeleteResult> {
        let req = DeleteReq {
            options: Some(delete_req::Options {
                stream_identifier: Some(stream_name.as_ref().into()),
                expected_stream_revision: Some(expected.into()),
            }),
        };

        let result = self
            .inner
            .delete(Request::new(req))
            .await?
            .into_inner()
            .result
            .expect("to be defined");

        match result {
            delete_resp::Result::Position(p) => Ok(DeleteResult::Success(p.into())),
            delete_resp::Result::WrongExpectedVersion(e) => Ok(
                DeleteResult::WrongExpectedRevision(WrongExpectedRevisionError {
                    expected: e.expected_revision_option.unwrap().into(),
                    current: e.current_revision_option.unwrap().into(),
                }),
            ),
        }
    }
}

pub struct ReadStream {
    inner: Streaming<ReadResp>,
}

impl ReadStream {
    pub async fn next(&mut self) -> eyre::Result<Option<Record>> {
        if let Some(item) = self.inner.try_next().await? {
            if let read_resp::Content::Event(item) = item.content.unwrap() {
                let item = item.event.unwrap();
                let r#type = item.metadata.get("type").cloned().unwrap_or_default();

                return Ok(Some(Record {
                    id: item
                        .id
                        .map_or_else(Uuid::nil, |x| x.try_into().unwrap_or_default()),
                    r#type,
                    stream_name: item.stream_identifier.unwrap().try_into()?,
                    position: Position(item.prepare_position),
                    revision: item.stream_revision,
                    data: item.data.into(),
                }));
            }
        }

        Ok(None)
    }
}
