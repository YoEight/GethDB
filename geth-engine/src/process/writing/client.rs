use crate::process::{
    messages::{WriteRequests, WriteResponses},
    ManagerClient, Proc, ProcId, ProcessRawEnv,
};
use geth_common::{
    AppendError, AppendStreamCompleted, ExpectedRevision, Position, Propose, WriteResult,
    WrongExpectedRevisionError,
};
use tracing::instrument;

#[derive(Clone)]
pub struct WriterClient {
    target: ProcId,
    inner: ManagerClient,
}

impl WriterClient {
    pub fn new(target: ProcId, inner: ManagerClient) -> Self {
        Self { target, inner }
    }

    pub fn from(env: &ProcessRawEnv) -> eyre::Result<Self> {
        let proc_id = env.handle.block_on(env.client.wait_for(Proc::Writing))?;
        Ok(Self::new(proc_id, env.client.clone()))
    }

    #[instrument(skip(self, events), fields(origin = ?self.inner.origin_proc))]
    pub async fn append(
        &self,
        stream: String,
        expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> eyre::Result<AppendStreamCompleted> {
        tracing::debug!("sending append request to writer process {}", self.target);
        let resp = self
            .inner
            .request(
                self.target,
                WriteRequests::Write {
                    ident: stream.clone(),
                    expected,
                    events,
                }
                .into(),
            )
            .await?;

        if let Ok(resp) = resp.payload.try_into() {
            match resp {
                WriteResponses::Error => {
                    eyre::bail!("internal error when appending to stream: '{}'", stream);
                }

                WriteResponses::StreamDeleted => {
                    Ok(AppendStreamCompleted::Error(AppendError::StreamDeleted))
                }

                WriteResponses::WrongExpectedRevision { expected, current } => Ok(
                    AppendStreamCompleted::Error(AppendError::WrongExpectedRevision(
                        WrongExpectedRevisionError { expected, current },
                    )),
                ),

                WriteResponses::Committed {
                    start_position: start,
                    next_position: next,
                    next_expected_version,
                } => {
                    tracing::debug!("completed successfully");

                    Ok(AppendStreamCompleted::Success(WriteResult {
                        next_expected_version,
                        position: Position(start),
                        next_logical_position: next,
                    }))
                }

                _ => eyre::bail!("unexpected response when appending to stream: '{}'", stream),
            }
        } else {
            eyre::bail!("internal protocol error when appending to the writer process");
        }
    }

    #[instrument(skip(self), fields(origin = ?self.inner.origin_proc))]
    pub async fn delete(
        &self,
        stream: String,
        expected: ExpectedRevision,
    ) -> eyre::Result<AppendStreamCompleted> {
        tracing::debug!("sending delete request to writer process {}", self.target);
        let resp = self
            .inner
            .request(
                self.target,
                WriteRequests::Delete {
                    ident: stream.clone(),
                    expected,
                }
                .into(),
            )
            .await?;

        if let Ok(resp) = resp.payload.try_into() {
            match resp {
                WriteResponses::Error => {
                    eyre::bail!("internal error when appending to stream: '{}'", stream);
                }

                WriteResponses::StreamDeleted => {
                    Ok(AppendStreamCompleted::Error(AppendError::StreamDeleted))
                }

                WriteResponses::WrongExpectedRevision { expected, current } => Ok(
                    AppendStreamCompleted::Error(AppendError::WrongExpectedRevision(
                        WrongExpectedRevisionError { expected, current },
                    )),
                ),

                WriteResponses::Committed {
                    start_position: start,
                    next_position: next,
                    next_expected_version,
                } => {
                    tracing::debug!("completed successfully");

                    Ok(AppendStreamCompleted::Success(WriteResult {
                        next_expected_version,
                        position: Position(start),
                        next_logical_position: next,
                    }))
                }

                _ => eyre::bail!("unexpected response when appending to stream: '{}'", stream),
            }
        } else {
            eyre::bail!("internal protocol error when appending to the writer process");
        }
    }
}
