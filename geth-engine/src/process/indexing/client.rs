use std::vec;

use crate::domain::index::CurrentRevision;
use crate::process::messages::{IndexRequests, IndexResponses, Messages, Requests};
use crate::process::{ManagerClient, Proc, ProcId, ProcessEnv, ProcessRawEnv};
use geth_common::{Direction, ReadCompleted};
use geth_domain::index::BlockEntry;
use tokio::sync::mpsc::UnboundedReceiver;

#[derive(Clone)]
pub struct IndexClient {
    target: ProcId,
    inner: ManagerClient,
}

impl IndexClient {
    pub fn new(target: ProcId, inner: ManagerClient) -> Self {
        Self { target, inner }
    }

    pub async fn resolve(env: &mut ProcessEnv) -> eyre::Result<Self> {
        tracing::debug!("waiting for the index process to be available...");
        let proc_id = env.client.wait_for(Proc::Indexing).await?;
        tracing::debug!("index process available on {}", proc_id);

        Ok(Self::new(proc_id, env.client.clone()))
    }

    pub fn resolve_raw(env: &mut ProcessRawEnv) -> eyre::Result<Self> {
        tracing::debug!("waiting the index process to be available...");
        let proc_id = env.handle.block_on(env.client.wait_for(Proc::Indexing))?;
        tracing::debug!("index process available on {}", proc_id);

        Ok(Self::new(proc_id, env.client.clone()))
    }

    pub async fn read(
        &mut self,
        key: u64,
        start: u64,
        count: usize,
        dir: Direction,
    ) -> eyre::Result<ReadCompleted<Streaming>> {
        let mut inner = self
            .inner
            .request_stream(
                self.target,
                Messages::Requests(Requests::Index(IndexRequests::Read {
                    key,
                    start,
                    count,
                    dir,
                })),
            )
            .await?;

        if let Some(resp) = inner.recv().await.and_then(Messages::into_index_response) {
            match resp {
                IndexResponses::Error => {
                    eyre::bail!("internal error when running a read request to the index process")
                }

                IndexResponses::StreamDeleted => return Ok(ReadCompleted::StreamDeleted),

                IndexResponses::Entries(entries) => {
                    return Ok(ReadCompleted::Success(Streaming {
                        inner,
                        batch: Some(entries.into_iter()),
                    }))
                }

                _ => {
                    eyre::bail!(
                        "unexpected response when running a read request to the index process"
                    )
                }
            }
        }

        eyre::bail!("index process is no longer reachable");
    }

    pub async fn store(&mut self, entries: Vec<BlockEntry>) -> eyre::Result<()> {
        let resp = self
            .inner
            .request(
                self.target,
                Messages::Requests(Requests::Index(IndexRequests::Store { entries })),
            )
            .await?;

        if let Some(resp) = resp.payload.into_index_response() {
            match resp {
                IndexResponses::Error => {
                    eyre::bail!("error when storing entries to the index process");
                }

                IndexResponses::Committed => return Ok(()),

                _ => {
                    eyre::bail!("unexpected response when storing entries to the index process");
                }
            }
        }

        eyre::bail!("unexpected message from the index process");
    }

    pub async fn latest_revision(&mut self, key: u64) -> eyre::Result<CurrentRevision> {
        let resp = self
            .inner
            .request(
                self.target,
                Messages::Requests(Requests::Index(IndexRequests::LatestRevision { key })),
            )
            .await?;

        if let Some(resp) = resp.payload.into_index_response() {
            match resp {
                IndexResponses::Error => {
                    eyre::bail!("error when fetching the latest revision from the index process");
                }

                IndexResponses::CurrentRevision(rev) => return Ok(rev),

                _ => {
                    eyre::bail!("unexpected response when fetching the latest revision from the index process");
                }
            }
        }

        eyre::bail!("unexpected message from the index process");
    }
}

pub struct Streaming {
    batch: Option<vec::IntoIter<BlockEntry>>,
    inner: UnboundedReceiver<Messages>,
}

impl Streaming {
    pub async fn next(&mut self) -> eyre::Result<Option<BlockEntry>> {
        loop {
            if let Some(entry) = self.batch.as_mut().and_then(Iterator::next) {
                return Ok(Some(entry));
            }

            self.batch = None;
            if let Some(resp) = self
                .inner
                .recv()
                .await
                .and_then(Messages::into_index_response)
            {
                match resp {
                    IndexResponses::Error => {
                        eyre::bail!("error when streaming from the index process");
                    }

                    IndexResponses::Entries(entries) => {
                        self.batch = Some(entries.into_iter());
                        continue;
                    }

                    _ => {
                        eyre::bail!("unexpected message when streaming from the index process");
                    }
                }
            }

            return Ok(None);
        }
    }

    pub async fn collect(&mut self) -> eyre::Result<Vec<BlockEntry>> {
        let mut entries = vec![];

        while let Some(entry) = self.next().await? {
            entries.push(entry);
        }

        Ok(entries)
    }
}
