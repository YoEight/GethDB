use crate::process::{
    messages::{WriteRequests, WriteResponses},
    ManagerClient, ProcId,
};
use geth_common::{
    AppendError, AppendStreamCompleted, ExpectedRevision, Position, Propose, WriteResult,
    WrongExpectedRevisionError,
};

#[derive(Clone)]
pub struct WriterClient {
    target: ProcId,
    inner: ManagerClient,
}

impl WriterClient {
    pub fn new(target: ProcId, inner: ManagerClient) -> Self {
        Self { target, inner }
    }

    pub async fn append(
        &self,
        stream: String,
        expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> eyre::Result<AppendStreamCompleted> {
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

        if let Some(resp) = resp.payload.try_into().ok() {
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
                } => Ok(AppendStreamCompleted::Success(WriteResult {
                    next_expected_version,
                    position: Position(start),
                    next_logical_position: next,
                })),
            }
        } else {
            eyre::bail!("internal protocol error when appending to the writer process");
        }
    }
}
