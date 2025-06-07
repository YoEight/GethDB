use geth_common::generated::protocol::protocol_client::ProtocolClient;
use geth_common::protocol::ProgramStatsRequest;
use tonic::transport::Channel;
use tonic::{Code, Request};

use geth_common::{
    AppendStream, AppendStreamCompleted, DeleteStream, DeleteStreamCompleted, Direction, EndPoint,
    ExpectedRevision, GetProgramError, KillProgram, ListPrograms, ProgramObtained, ProgramStats,
    ProgramSummary, Propose, ReadError, ReadStream, ReadStreamCompleted, Revision, Subscribe,
    SubscribeToProgram, SubscribeToStream,
};

use crate::{Client, ReadStreaming, SubscriptionStreaming};

#[derive(Clone)]
pub struct GrpcClient {
    inner: ProtocolClient<Channel>,
}

impl GrpcClient {
    pub async fn connect(endpoint: EndPoint) -> eyre::Result<Self> {
        let inner = ProtocolClient::connect(format!("http://{}", endpoint)).await?;

        Ok(Self { inner })
    }
}

#[async_trait::async_trait]
impl Client for GrpcClient {
    async fn append_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
        proposes: Vec<Propose>,
    ) -> eyre::Result<AppendStreamCompleted> {
        let result = self
            .inner
            .clone()
            .append_stream(Request::new(
                AppendStream {
                    stream_name: stream_id.to_string(),
                    expected_revision,
                    events: proposes,
                }
                .into(),
            ))
            .await?;

        Ok(result.into_inner().into())
    }

    async fn read_stream(
        &self,
        stream_id: &str,
        direction: Direction,
        revision: Revision<u64>,
        max_count: u64,
    ) -> eyre::Result<ReadStreamCompleted<ReadStreaming>> {
        let result = self
            .inner
            .clone()
            .read_stream(Request::new(
                ReadStream {
                    stream_name: stream_id.to_string(),
                    direction,
                    revision,
                    max_count,
                }
                .into(),
            ))
            .await;

        match result {
            Err(s) => match parse_read_error(s)? {
                ReadError::StreamDeleted => Ok(ReadStreamCompleted::StreamDeleted),
            },

            Ok(resp) => Ok(ReadStreamCompleted::Success(ReadStreaming::Grpc(
                resp.into_inner(),
            ))),
        }
    }

    async fn subscribe_to_stream(
        &self,
        stream_id: &str,
        start: Revision<u64>,
    ) -> eyre::Result<SubscriptionStreaming> {
        let result = self
            .inner
            .clone()
            .subscribe(Request::new(
                Subscribe::ToStream(SubscribeToStream {
                    stream_name: stream_id.to_string(),
                    start,
                })
                .into(),
            ))
            .await?;

        Ok(SubscriptionStreaming::Grpc(result.into_inner()))
    }

    async fn subscribe_to_process(
        &self,
        name: &str,
        source_code: &str,
    ) -> eyre::Result<SubscriptionStreaming> {
        let result = self
            .inner
            .clone()
            .subscribe(Request::new(
                Subscribe::ToProgram(SubscribeToProgram {
                    name: name.to_string(),
                    source: source_code.to_string(),
                })
                .into(),
            ))
            .await?;

        Ok(SubscriptionStreaming::Grpc(result.into_inner()))
    }

    async fn delete_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
    ) -> eyre::Result<DeleteStreamCompleted> {
        let result = self
            .inner
            .clone()
            .delete_stream(Request::new(
                DeleteStream {
                    stream_name: stream_id.to_string(),
                    expected_revision,
                }
                .into(),
            ))
            .await?;

        Ok(result.into_inner().into())
    }

    async fn list_programs(&self) -> eyre::Result<Vec<ProgramSummary>> {
        let result = self
            .inner
            .clone()
            .list_programs(Request::new(ListPrograms {}.into()))
            .await?;

        // paying a premium just so we have a type that is not from the generated code
        // fortunately, that isn't a call that one should make often.
        Ok(result
            .into_inner()
            .programs
            .into_iter()
            .map(|p| p.into())
            .collect())
    }

    async fn get_program(&self, id: u64) -> eyre::Result<Option<ProgramStats>> {
        let result = self
            .inner
            .clone()
            .program_stats(Request::new(ProgramStatsRequest { id }.into()))
            .await?;

        match result.into_inner().into() {
            ProgramObtained::Success(stats) => Ok(Some(stats)),
            ProgramObtained::Error(e) => match e {
                GetProgramError::NotExists => Ok(None),
            },
        }
    }

    async fn stop_program(&self, id: u64) -> eyre::Result<()> {
        self.inner
            .clone()
            .stop_program(Request::new(KillProgram { id }.into()))
            .await?;

        Ok(())
    }
}

fn parse_read_error(status: tonic::Status) -> eyre::Result<ReadError> {
    if status.code() == Code::FailedPrecondition && status.message() == "stream-deleted" {
        Ok(ReadError::StreamDeleted)
    } else {
        Err(status.into())
    }
}
