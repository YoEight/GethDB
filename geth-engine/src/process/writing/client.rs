use crate::process::{
    ManagerClient, ProcId, RequestContext,
    messages::{WriteRequests, WriteResponses},
};
use geth_common::{
    AppendError, AppendStreamCompleted, DeleteError, DeleteStreamCompleted, ExpectedRevision,
    Propose, WriteResult, WrongExpectedRevisionError,
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

    #[instrument(skip(self, events, context), fields(origin = ?self.inner.origin(), correlation = %context.correlation))]
    pub async fn append(
        &self,
        context: RequestContext,
        stream: String,
        expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> eyre::Result<AppendStreamCompleted> {
        let resp = self
            .inner
            .request(
                context,
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
                    tracing::debug!(correlation = %context.correlation, "completed successfully");

                    Ok(AppendStreamCompleted::Success(WriteResult {
                        next_expected_version,
                        position: start,
                        next_logical_position: next,
                    }))
                }

                _ => eyre::bail!("unexpected response when appending to stream: '{}'", stream),
            }
        } else {
            eyre::bail!("internal error: writer process is unaivailable");
        }
    }

    #[instrument(skip(self, context), fields(origin = ?self.inner.origin(), correlation = %context.correlation))]
    pub async fn delete(
        &self,
        context: RequestContext,
        stream: String,
        expected: ExpectedRevision,
    ) -> eyre::Result<DeleteStreamCompleted> {
        let resp = self
            .inner
            .request(
                context,
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
                    Ok(DeleteStreamCompleted::Error(DeleteError::StreamDeleted))
                }

                WriteResponses::WrongExpectedRevision { expected, current } => Ok(
                    DeleteStreamCompleted::Error(DeleteError::WrongExpectedRevision(
                        WrongExpectedRevisionError { expected, current },
                    )),
                ),

                WriteResponses::Committed {
                    start_position: start,
                    next_position: next,
                    next_expected_version,
                } => Ok(DeleteStreamCompleted::Success(WriteResult {
                    next_expected_version,
                    position: start,
                    next_logical_position: next,
                })),

                _ => eyre::bail!("unexpected response when appending to stream: '{}'", stream),
            }
        } else {
            eyre::bail!("internal protocol error when appending to the writer process");
        }
    }
}
