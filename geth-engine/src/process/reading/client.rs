use crate::messages::ReadStreamCompleted;
use crate::process::reading::{LogEntryExt, Request, Response};
use crate::process::{ManagerClient, ProcId, ProcessEnv};
use bytes::{Bytes, BytesMut};
use geth_common::{Direction, Revision};
use geth_mikoshi::wal::LogEntry;
use tokio::sync::mpsc::UnboundedReceiver;

pub struct Streaming {
    inner: UnboundedReceiver<Bytes>,
    batch: Option<Bytes>,
}

impl Streaming {
    pub fn from(inner: UnboundedReceiver<Bytes>) -> Self {
        Self { inner, batch: None }
    }

    pub async fn next(&mut self) -> Option<LogEntry> {
        loop {
            if let Some(batch) = self.batch.as_mut() {
                if !batch.is_empty() {
                    return Some(LogEntry::deserialize(batch));
                }
            }

            self.batch = None;
            if let Some(bytes) = self.inner.recv().await {
                self.batch = Some(bytes);
                continue;
            }

            return None;
        }
    }
}

pub struct ReaderClient {
    target: ProcId,
    inner: ManagerClient,
    buffer: BytesMut,
}

impl ReaderClient {
    pub fn new(target: ProcId, inner: ManagerClient, buffer: BytesMut) -> Self {
        Self {
            target,
            inner,
            buffer,
        }
    }

    pub async fn resolve(env: &mut ProcessEnv) -> eyre::Result<Self> {
        tracing::debug!("waiting for the reader process to be available...");
        let proc_id = env.client.wait_for("reader").await?;
        tracing::debug!("reader process available on {}", proc_id);

        Ok(Self::new(proc_id, env.client.clone(), env.buffer.split()))
    }

    pub async fn read(
        &mut self,
        stream_name: &str,
        start: Revision<u64>,
        direction: Direction,
        count: usize,
    ) -> eyre::Result<ReadStreamCompleted> {
        let mut mailbox = self
            .inner
            .request_stream(
                self.target,
                Request::read(&mut self.buffer, stream_name, start, direction, count),
            )
            .await?;

        if let Some(resp) = mailbox.recv().await {
            if let Some(resp) = Response::try_from(resp) {
                match resp {
                    Response::Error => {
                        eyre::bail!("internal error");
                    }

                    Response::StreamDeleted => {
                        return Ok(ReadStreamCompleted::StreamDeleted);
                    }

                    Response::Streaming => {
                        return Ok(ReadStreamCompleted::Success(Streaming {
                            inner: mailbox,
                            batch: None,
                        }));
                    }
                }
            }

            eyre::bail!("protocol error when communicating with the reader process");
        }

        eyre::bail!("reader process is no longer running")
    }
}
