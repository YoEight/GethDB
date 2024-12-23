use crate::process::writing::{Request, Response};
use crate::process::{ManagerClient, ProcId, ProcessEnv};
use bytes::{Bytes, BytesMut};
use geth_common::{
    AppendError, AppendStreamCompleted, ExpectedRevision, Position, WriteResult,
    WrongExpectedRevisionError,
};
use uuid::Uuid;

pub struct WriterClient {
    target: ProcId,
    inner: ManagerClient,
    buffer: BytesMut,
}

impl WriterClient {
    pub fn new(target: ProcId, inner: ManagerClient, buffer: BytesMut) -> Self {
        Self {
            target,
            inner,
            buffer,
        }
    }

    pub async fn resolve(env: &mut ProcessEnv) -> eyre::Result<Self> {
        tracing::debug!("waiting for process for the writer process to be available...");
        let proc_id = env.client.wait_for("writer").await?;
        tracing::debug!("writer process available on {}", proc_id);

        Ok(Self::new(proc_id, env.client.clone(), env.buffer.split()))
    }

    pub async fn append<I>(
        &mut self,
        stream: &str,
        expected: ExpectedRevision,
        index: bool,
        entries: I,
    ) -> eyre::Result<AppendStreamCompleted>
    where
        I: IntoIterator<Item = Bytes>,
    {
        let mut builder = Request::append_builder(&mut self.buffer, stream, expected, index);

        for payload in entries {
            builder.push(&payload);
        }

        let mut resp = self.inner.request(self.target, builder.build()).await?;
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
                Response::Committed { start, next } => {
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
