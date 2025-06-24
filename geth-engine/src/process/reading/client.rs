use crate::process::messages::{Messages, ReadRequests, ReadResponses};
use crate::process::reading::record_try_from;
use crate::process::{Managed, ManagerClient, Proc, ProcId, ProcessEnv, RequestContext};
use geth_common::{Direction, ReadStreamCompleted, Record, Revision};
use geth_mikoshi::wal::LogEntry;
use std::vec;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tracing::instrument;

pub struct Streaming {
    inner: UnboundedReceiver<Messages>,
    batch: Option<vec::IntoIter<LogEntry>>,
}

impl Streaming {
    pub fn empty() -> Self {
        Self {
            inner: mpsc::unbounded_channel().1,
            batch: None,
        }
    }

    pub async fn next(&mut self) -> eyre::Result<Option<Record>> {
        loop {
            if let Some(entry) = self.batch.as_mut().and_then(Iterator::next) {
                return Ok(Some(record_try_from(entry)?));
            }

            self.batch = None;
            if let Some(resp) = self.inner.recv().await.and_then(|m| m.try_into().ok()) {
                match resp {
                    ReadResponses::Error => {
                        eyre::bail!("error when streaming from the reader process");
                    }

                    ReadResponses::Entries(entries) => {
                        self.batch = Some(entries.into_iter());
                        continue;
                    }

                    _ => {
                        eyre::bail!("unexpected message when streaming from the reader process");
                    }
                }
            }

            return Ok(None);
        }
    }
}

#[derive(Clone)]
pub struct ReaderClient {
    target: ProcId,
    inner: ManagerClient,
}

impl ReaderClient {
    pub fn new(target: ProcId, inner: ManagerClient) -> Self {
        Self { target, inner }
    }

    pub async fn resolve(env: &mut ProcessEnv<Managed>) -> eyre::Result<Self> {
        tracing::debug!("waiting for the reader process to be available...");
        let proc_id = env.client.wait_for(Proc::Reading).await?.must_succeed()?;
        tracing::debug!("reader process available on {}", proc_id);

        Ok(Self::new(proc_id, env.client.clone()))
    }

    pub fn manager(&self) -> ManagerClient {
        self.inner.clone()
    }

    #[instrument(skip(self, context), fields(correlation = %context.correlation))]
    pub async fn read(
        &self,
        context: RequestContext,
        stream_name: &str,
        start: Revision<u64>,
        direction: Direction,
        count: usize,
    ) -> eyre::Result<ReadStreamCompleted<Streaming>> {
        let mut mailbox = self
            .inner
            .request_stream(
                context,
                self.target,
                ReadRequests::Read {
                    ident: stream_name.to_string(),
                    start: start.raw(),
                    direction,
                    count,
                }
                .into(),
            )
            .await?;

        if let Some(resp) = mailbox.recv().await {
            if let Ok(resp) = resp.try_into() {
                match resp {
                    ReadResponses::Error => {
                        eyre::bail!(
                            "internal error when running a read request to the reader process"
                        );
                    }

                    ReadResponses::StreamDeleted => {
                        return Ok(ReadStreamCompleted::StreamDeleted);
                    }

                    ReadResponses::Entries(entries) => {
                        return Ok(ReadStreamCompleted::Success(Streaming {
                            inner: mailbox,
                            batch: Some(entries.into_iter()),
                        }));
                    }

                    _ => {
                        eyre::bail!("protocol error when communicating with the reader process");
                    }
                }
            }
        }

        eyre::bail!("reader process is no longer running")
    }

    #[instrument(skip(self, context), fields(correlation = %context.correlation))]
    pub async fn read_at(&self, context: RequestContext, position: u64) -> eyre::Result<LogEntry> {
        let resp = self
            .inner
            .request(
                context,
                self.target,
                ReadRequests::ReadAt { position }.into(),
            )
            .await?;

        if let Ok(resp) = resp.payload.try_into() {
            match resp {
                ReadResponses::Error => {
                    eyre::bail!("unexpected error when reading from the reader process");
                }

                ReadResponses::Entry(entry) => {
                    return Ok(entry);
                }

                _ => {
                    eyre::bail!("protocol error when communicating with the reader process");
                }
            }
        }

        eyre::bail!("unexpected response from the reader process")
    }
}
