mod types;

use geth_common::{
    protocol::streams::{
        append_req::{self, ProposedMessage},
        append_resp,
        client::StreamsClient,
        AppendReq,
    },
    ExpectedRevision, Propose, WriteResult, WrongExpectedRevisionError,
};
use tonic::{
    codegen::StdError,
    transport::{self, Channel, Endpoint},
};

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
            msgs.push(AppendReq {
                content: Some(append_req::Content::ProposedMessage(ProposedMessage {
                    id: Some(uuid::Uuid::new_v4().into()),
                    metadata: Default::default(),
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
                })
            }
            append_resp::Result::WrongExpectedVersion(err) => {
                let expected = err.expected_revision_option.unwrap().into();
                let current = err.current_revision_option.unwrap().into();

                Err(WrongExpectedRevisionError { expected, current }.into())
            }
        }
    }
}
