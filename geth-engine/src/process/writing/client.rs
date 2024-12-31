use crate::process::writing::{Request, Response};
use crate::process::{ManagerClient, ProcId};
use bytes::Bytes;
use geth_common::{
    AppendError, AppendStreamCompleted, ExpectedRevision, Position, WriteResult,
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

    pub async fn append<I>(
        &self,
        stream: &str,
        expected: ExpectedRevision,
        index: bool,
        entries: I,
    ) -> eyre::Result<AppendStreamCompleted>
    where
        I: IntoIterator<Item = Bytes>,
    {
        let mut buffer = self.inner.pool.get().await.unwrap();
        let mut builder = Request::append_builder(&mut buffer, stream, expected, index);

        for payload in entries {
            builder.push(&payload);
        }

        let resp = self.inner.request(self.target, builder.build()).await?;
        if let Some(resp) = Response::try_from(resp.payload) {
            match resp {
                Response::Error => {
                    eyre::bail!("internal error when appending to stream: '{}'", stream);
                }

                Response::Deleted => Ok(AppendStreamCompleted::Error(AppendError::StreamDeleted)),

                Response::WrongExpectedRevision { expected, current } => Ok(
                    AppendStreamCompleted::Error(AppendError::WrongExpectedRevision(
                        WrongExpectedRevisionError { expected, current },
                    )),
                ),
                Response::Committed { start, next: _next } => {
                    Ok(AppendStreamCompleted::Success(WriteResult {
                        next_expected_version: ExpectedRevision::NoStream,
                        position: Position(start),
                        next_logical_position: 0,
                    }))
                }
            }
        } else {
            eyre::bail!("internal protocol error when appending to the writer process");
        }
    }
}
