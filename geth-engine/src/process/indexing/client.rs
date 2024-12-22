use crate::domain::index::CurrentRevision;
use crate::process::indexing::{Request, Response};
use crate::process::{ManagerClient, ProcId, ProcessEnv, ProcessRawEnv};
use bytes::{Buf, Bytes, BytesMut};
use geth_common::{Direction, ReadCompleted};
use tokio::sync::mpsc::UnboundedReceiver;
use uuid::Uuid;

pub struct IndexClient {
    target: ProcId,
    inner: ManagerClient,
    buffer: BytesMut,
}

impl IndexClient {
    pub fn new(target: ProcId, inner: ManagerClient, buffer: BytesMut) -> Self {
        Self {
            target,
            inner,
            buffer,
        }
    }

    pub async fn resolve(env: &mut ProcessEnv) -> eyre::Result<Self> {
        let proc_id = env.client.wait_for("index").await?;

        Ok(Self::new(proc_id, env.client.clone(), env.buffer.split()))
    }

    pub fn resolve_raw(env: &mut ProcessRawEnv) -> eyre::Result<Self> {
        let proc_id = env.handle.block_on(env.client.wait_for("index"))?;

        Ok(Self::new(proc_id, env.client.clone(), env.buffer.split()))
    }

    pub async fn read(
        &mut self,
        key: u64,
        start: u64,
        count: usize,
        dir: Direction,
    ) -> eyre::Result<ReadCompleted<Streaming>> {
        let payload = Request::read(self.buffer.split(), key, start, count, dir);
        let mut inner = self.inner.request_stream(self.target, payload).await?;

        if let Some(bytes) = inner.recv().await {
            if bytes.len() != 1 {
                eyre::bail!("unexpected message from the index process");
            }

            match Response::try_from(bytes)? {
                Response::Error => {
                    eyre::bail!("internal error when running a read request to the index process")
                }

                Response::StreamDeleted => return Ok(ReadCompleted::StreamDeleted),

                Response::Streaming => {
                    return Ok(ReadCompleted::Success(Streaming { inner, batch: None }))
                }

                _ => eyre::bail!("unexpected message when reading from the index process"),
            }
        }

        eyre::bail!("index process is no longer reachable");
    }

    pub async fn store<I>(&mut self, key: u64, entries: I) -> eyre::Result<()>
    where
        I: IntoIterator<Item = (u64, u64)>,
    {
        let mut req = Request::store(self.buffer.split(), key);

        for (revision, position) in entries {
            req.put_entry(revision, position);
        }

        let resp = self.inner.request(self.target, req.build()).await?;

        Response::try_from(resp.payload)?.expect(Response::Committed)
    }

    /// You send pre-serialized index request so you know what you are doing. Most likely we need
    /// another way to construct index request if you use that function.
    pub async fn store_raw(&mut self, payload: Bytes) -> eyre::Result<()> {
        let resp = self.inner.request(self.target, payload).await?;
        Response::try_from(resp.payload)?.expect(Response::Committed)
    }

    pub async fn latest_revision(&mut self, key: u64) -> eyre::Result<CurrentRevision> {
        let req = Request::latest_revision(self.buffer.split(), key);
        let mut resp = self.inner.request(self.target, req).await?;

        if resp.payload.len() == 1 {
            eyre::bail!("error when looking the latest version of key {}", key);
        }

        if resp.payload.is_empty() {
            return Ok(CurrentRevision::NoStream);
        }

        Ok(CurrentRevision::Revision(resp.payload.get_u64_le()))
    }
}

pub struct Streaming {
    batch: Option<Bytes>,
    inner: UnboundedReceiver<Bytes>,
}

impl Streaming {
    pub async fn next(&mut self) -> eyre::Result<Option<(u64, u64)>> {
        loop {
            if let Some(bytes) = self.batch.as_mut() {
                if bytes.has_remaining() {
                    return Ok(Some((bytes.get_u64_le(), bytes.get_u64_le())));
                }
            }

            self.batch = None;
            if let Some(bytes) = self.inner.recv().await {
                if bytes.len() == 1 {
                    Response::try_from(bytes)?.expect(Response::Error)?;
                    eyre::bail!("error when streaming from the index process");
                }

                self.batch = Some(bytes);
                continue;
            }

            return Ok(None);
        }
    }

    pub async fn collect(&mut self) -> eyre::Result<Vec<(u64, u64)>> {
        let mut entries = vec![];

        while let Some(entry) = self.next().await? {
            entries.push(entry);
        }

        Ok(entries)
    }
}
