use std::vec;

use crate::messages::ReadStreamCompleted;
use crate::process::messages::{Messages, ReadRequests, ReadResponses};
use crate::process::{ManagerClient, Proc, ProcId, ProcessEnv};
use geth_common::{Direction, Revision};
use geth_mikoshi::wal::LogEntry;
use tokio::sync::mpsc::UnboundedReceiver;

pub struct Streaming {
    inner: UnboundedReceiver<Messages>,
    batch: Option<vec::IntoIter<LogEntry>>,
}

impl Streaming {
    pub async fn next(&mut self) -> eyre::Result<Option<LogEntry>> {
        loop {
            if let Some(entry) = self.batch.as_mut().and_then(Iterator::next) {
                return Ok(Some(entry));
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

    pub async fn resolve(env: &mut ProcessEnv) -> eyre::Result<Self> {
        tracing::debug!("waiting for the reader process to be available...");
        let proc_id = env.client.wait_for(Proc::Reading).await?;
        tracing::debug!("reader process available on {}", proc_id);

        Ok(Self::new(proc_id, env.client.clone()))
    }

    pub async fn read(
        &self,
        stream_name: &str,
        start: Revision<u64>,
        direction: Direction,
        count: usize,
    ) -> eyre::Result<ReadStreamCompleted> {
        let mut mailbox = self
            .inner
            .request_stream(
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
            if let Some(resp) = resp.try_into().ok() {
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
                }
            }

            eyre::bail!("protocol error when communicating with the reader process");
        }

        eyre::bail!("reader process is no longer running")
    }
}
