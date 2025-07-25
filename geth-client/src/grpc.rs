use std::time::Duration;

use geth_grpc::generated::protocol::protocol_client::ProtocolClient;
use geth_grpc::protocol::ProgramStatsRequest;
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Uri};
use tonic::{Code, Request};

use geth_common::{
    AppendStream, AppendStreamCompleted, DeleteStream, DeleteStreamCompleted, Direction, EndPoint,
    ExpectedRevision, GetProgramError, KillProgram, ListPrograms, ProgramObtained, ProgramStats,
    ProgramSummary, Propose, ReadError, ReadStream, ReadStreamCompleted, Revision, Subscribe,
    SubscribeToProgram, SubscribeToStream,
};

use crate::{Client, ReadStreaming, SubscriptionStreaming};

#[derive(Debug, Clone, Copy)]
struct CorrelationInjectionInterceptor;

impl Interceptor for CorrelationInjectionInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        request.metadata_mut().insert(
            "correlation",
            uuid::Uuid::new_v4().to_string().parse().unwrap(),
        );

        Ok(request)
    }
}

#[derive(Clone)]
pub struct GrpcClient {
    inner: ProtocolClient<InterceptedService<Channel, CorrelationInjectionInterceptor>>,
}

impl GrpcClient {
    pub async fn connect(endpoint: EndPoint) -> eyre::Result<Self> {
        let max_attempts = 10;
        let mut attempt = 1;

        while attempt <= max_attempts {
            tracing::debug!(
                endpoint = %endpoint,
                attempt = attempt,
                max_attempts = max_attempts,
                "connecting to node"
            );

            let uri = format!("http://{}:{}", endpoint.host, endpoint.port).parse::<Uri>()?;
            match Channel::builder(uri.clone()).connect().await {
                Err(e) => {
                    tracing::warn!(attempt = attempt, max_attempts = max_attempts, error = %e, "failed to connect to node");
                    attempt += 1;

                    tokio::time::sleep(Duration::from_millis(500)).await;
                }

                Ok(channel) => {
                    tracing::debug!(attempt = attempt, max_attempts = max_attempts, endpoint = %endpoint, "connected to node");
                    let inner =
                        ProtocolClient::with_interceptor(channel, CorrelationInjectionInterceptor);
                    return Ok(Self { inner });
                }
            }
        }

        eyre::bail!("cannot connect to {}", endpoint)
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

        Ok(result.into_inner().try_into()?)
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

        Ok(SubscriptionStreaming::from_grpc(result.into_inner()))
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

        let stream = result.into_inner();

        tracing::debug!(
            name = name,
            "waiting for subscription to process confirmation"
        );

        Ok(SubscriptionStreaming::from_grpc(stream))
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

        Ok(result.into_inner().try_into()?)
    }

    async fn list_programs(&self) -> eyre::Result<Vec<ProgramSummary>> {
        let result = self
            .inner
            .clone()
            .list_programs(Request::new(ListPrograms {}.into()))
            .await?;

        // paying a premium just so we have a type that is not from the generated code
        // fortunately, that isn't a call that one should make often.
        let res: Result<Vec<ProgramSummary>, tonic::Status> = result
            .into_inner()
            .programs
            .into_iter()
            .map(|p| p.try_into())
            .collect();

        Ok(res?)
    }

    async fn get_program(&self, id: u64) -> eyre::Result<Option<ProgramStats>> {
        let result = self
            .inner
            .clone()
            .program_stats(Request::new(ProgramStatsRequest { id }))
            .await;

        match result {
            Err(e) => {
                if e.code() == Code::NotFound {
                    Ok(None)
                } else {
                    Err(e.into())
                }
            }

            Ok(resp) => match resp.into_inner().try_into()? {
                ProgramObtained::Success(stats) => Ok(Some(stats)),
                ProgramObtained::Error(e) => match e {
                    GetProgramError::NotExists => Ok(None),
                },
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
