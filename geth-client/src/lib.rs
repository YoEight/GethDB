mod types;

use futures_util::TryStreamExt;
use geth_common::protocol::streams::read_req::options::subscription_options::SubKind;
use geth_common::protocol::streams::read_req::options::{Programmable, SubscriptionOptions};
use geth_common::protocol::Empty;
use geth_common::{
    protocol::streams::{
        append_req::{self, ProposedMessage},
        append_resp,
        client::StreamsClient,
        read_req::{self, options::StreamOptions},
        read_resp, AppendReq, ReadReq, ReadResp,
    },
    Direction, ExpectedRevision, Position, Propose, Record, Revision, WriteResult,
    WrongExpectedRevisionError,
};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{
    codegen::StdError,
    transport::{self, Channel, Endpoint},
    Request, Streaming,
};
use uuid::Uuid;

#[derive(Default)]
pub struct ClientBuilder {
    tenant_id: Option<String>,
}

impl ClientBuilder {
    pub fn tenant_id(mut self, tenant_id: impl AsRef<str>) -> Self {
        self.tenant_id = Some(tenant_id.as_ref().to_string());
        self
    }

    pub async fn build<D>(self, dest: D) -> Result<Client, transport::Error>
    where
        D: TryInto<Endpoint>,
        D::Error: Into<StdError>,
    {
        let inner = StreamsClient::connect(dest).await?;

        Ok(Client {
            tenant_id: Arc::new(self.tenant_id.unwrap_or_default()),
            inner,
        })
    }
}

#[derive(Clone)]
pub struct Client {
    tenant_id: Arc<String>,
    inner: StreamsClient<Channel>,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
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
                tenant_id: self.tenant_id.to_string(),
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
                        tenant_id: self.tenant_id.to_string(),
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
    ) -> eyre::Result<ReadStream> {
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
                        tenant_id: self.tenant_id.to_string(),
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
                        tenant_id: self.tenant_id.to_string(),
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
