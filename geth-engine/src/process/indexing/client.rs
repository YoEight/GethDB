use std::vec;

use crate::domain::index::CurrentRevision;
use crate::process::messages::{IndexRequests, IndexResponses, Messages, Requests};
use crate::process::{ManagerClient, Proc, ProcId, ProcessEnv, ProcessRawEnv, RequestContext};
use geth_common::{Direction, ReadCompleted};
use geth_domain::index::BlockEntry;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::instrument;

#[derive(Debug, Clone)]
pub struct IndexClient {
    target: ProcId,
    inner: ManagerClient,
}

impl IndexClient {
    pub fn new(target: ProcId, inner: ManagerClient) -> Self {
        Self { target, inner }
    }

    pub async fn resolve(env: &mut ProcessEnv) -> eyre::Result<Self> {
        let proc_id = env.client.wait_for(Proc::Indexing).await?.must_succeed()?;
        Ok(Self::new(proc_id, env.client.clone()))
    }

    pub fn resolve_raw(env: &ProcessRawEnv) -> eyre::Result<Self> {
        let proc_id = env
            .handle
            .block_on(env.client.wait_for(Proc::Indexing))?
            .must_succeed()?;
        Ok(Self::new(proc_id, env.client.clone()))
    }

    #[instrument(skip(self), fields(origin = ?self.inner.origin_proc, correlation = %context.correlation))]
    pub async fn read(
        &self,
        context: RequestContext,
        key: u64,
        start: u64,
        count: usize,
        dir: Direction,
    ) -> eyre::Result<ReadCompleted<Streaming>> {
        let mut inner = self
            .inner
            .request_stream(
                context,
                self.target,
                Messages::Requests(Requests::Index(IndexRequests::Read {
                    key,
                    start,
                    count,
                    dir,
                })),
            )
            .await?;

        if let Some(resp) = inner.recv().await.and_then(|m| m.try_into().ok()) {
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

    #[instrument(skip(self, entries), fields(origin = ?self.inner.origin_proc, correlation = %context.correlation))]
    pub async fn store(
        &self,
        context: RequestContext,
        entries: Vec<BlockEntry>,
    ) -> eyre::Result<()> {
        let resp = self
            .inner
            .request(
                context,
                self.target,
                Messages::Requests(Requests::Index(IndexRequests::Store { entries })),
            )
            .await?;

        if let Ok(resp) = resp.payload.try_into() {
            match resp {
                IndexResponses::Error => {
                    eyre::bail!("error when storing entries to the index process");
                }

                IndexResponses::Committed => {
                    return Ok(());
                }

                _ => {
                    eyre::bail!("unexpected response when storing entries to the index process");
                }
            }
        }

        eyre::bail!("unexpected message from the index process");
    }

    #[instrument(skip(self), fields(origin = ?self.inner.origin_proc, correlation = %context.correlation))]
    pub async fn latest_revision(
        &self,
        context: RequestContext,
        key: u64,
    ) -> eyre::Result<CurrentRevision> {
        let resp = self
            .inner
            .request(
                context,
                self.target,
                Messages::Requests(Requests::Index(IndexRequests::LatestRevision { key })),
            )
            .await?;

        if let Ok(resp) = resp.payload.try_into() {
            match resp {
                IndexResponses::Error => {
                    eyre::bail!("error when fetching the latest revision from the index process");
                }

                IndexResponses::CurrentRevision(rev) => {
                    return Ok(rev);
                }

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
            if let Some(resp) = self.inner.recv().await.and_then(|m| m.try_into().ok()) {
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
