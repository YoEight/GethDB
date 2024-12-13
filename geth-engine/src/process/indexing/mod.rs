use super::{Item, ManagerClient, ProcessEnv, ProcessRawEnv, RunnableRaw};
use crate::domain::index::CurrentRevision;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use geth_common::{Direction, IteratorIO};
use geth_domain::index::BlockEntry;
use geth_domain::{Lsm, LsmSettings};
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use uuid::Uuid;

type RevisionCache = moka::sync::Cache<u64, u64>;

pub fn new_revision_cache() -> RevisionCache {
    moka::sync::Cache::<u64, u64>::builder()
        .max_capacity(10_000)
        .name("revision-cache")
        .build()
}

pub struct Streaming {
    inner: UnboundedReceiver<Bytes>,
}

impl Streaming {
    pub async fn next(&mut self) -> eyre::Result<Option<Bytes>> {
        if let Some(bytes) = self.inner.recv().await {
            if bytes.len() == 1 {
                IndexingResp::try_from(bytes)?.expect(IndexingResp::Error)?;
                eyre::bail!("error when streaming from the index process");
            }

            return Ok(Some(bytes));
        }

        Ok(None)
    }
}

pub struct IndexClient {
    target: Uuid,
    inner: ManagerClient,
    buffer: BytesMut,
}

impl IndexClient {
    pub fn new(target: Uuid, inner: ManagerClient, buffer: BytesMut) -> Self {
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

    pub async fn read(
        &mut self,
        key: u64,
        start: u64,
        count: usize,
        dir: Direction,
    ) -> eyre::Result<Streaming> {
        let payload = IndexingReq::read(self.buffer.split(), key, start, count, dir);
        let mut inner = self.inner.request_stream(self.target, payload).await?;

        if let Some(bytes) = inner.recv().await {
            if bytes.len() != 1 {
                eyre::bail!("unexpected message from the index process");
            }

            IndexingResp::try_from(bytes)?.expect(IndexingResp::Streaming)?;

            return Ok(Streaming { inner });
        }

        eyre::bail!("index process is no longer reachable");
    }

    pub async fn store<I>(&mut self, key: u64, entries: I) -> eyre::Result<()>
    where
        I: IntoIterator<Item = (u64, u64)>,
    {
        let mut req = IndexingReq::store(self.buffer.split(), key);

        for (revision, position) in entries {
            req.put_entry(revision, position);
        }

        let resp = self.inner.request(self.target, req.build()).await?;

        IndexingResp::try_from(resp.payload)?.expect(IndexingResp::Committed)
    }

    pub async fn latest_revision(&mut self, key: u64) -> eyre::Result<u64> {
        let req = IndexingReq::latest_revision(self.buffer.split(), key);
        let mut resp = self.inner.request(self.target, req).await?;

        if resp.payload.len() == 1 {
            eyre::bail!("error when looking the latest version of key {}", key);
        }

        Ok(resp.payload.get_u64_le())
    }
}

pub struct StoreRequestBuilder {
    inner: BytesMut,
}

impl StoreRequestBuilder {
    pub fn new(mut inner: BytesMut, key: u64) -> Self {
        inner.put_u8(0x01);
        inner.put_u64_le(key);

        Self { inner }
    }

    pub fn put_entry(&mut self, revision: u64, position: u64) {
        self.inner.put_u64_le(revision);
        self.inner.put_u64_le(position);
    }

    pub fn build(self) -> Bytes {
        self.inner.freeze()
    }
}

enum IndexingReq {
    Read {
        key: u64,
        start: u64,
        count: u64,
        dir: Direction,
    },

    Store {
        key: u64,
        entries: Bytes,
    },

    LatestRevision {
        key: u64,
    },
}

impl IndexingReq {
    fn try_from(mut bytes: Bytes) -> Option<Self> {
        match bytes.get_u8() {
            0x00 => Some(Self::Read {
                key: bytes.get_u64_le(),
                start: bytes.get_u64_le(),
                count: bytes.get_u64_le(),
                dir: match bytes.get_u8() {
                    0x00 => Direction::Forward,
                    0x01 => Direction::Backward,
                    _ => unreachable!(),
                },
            }),

            0x01 => Some(Self::Store {
                key: bytes.get_u64_le(),
                entries: bytes,
            }),

            0x02 => Some(Self::LatestRevision {
                key: bytes.get_u64_le(),
            }),

            _ => unreachable!(),
        }
    }

    pub fn read(mut buffer: BytesMut, key: u64, start: u64, count: usize, dir: Direction) -> Bytes {
        buffer.put_u8(0x00);
        buffer.put_u64_le(key);
        buffer.put_u64_le(start);
        buffer.put_u64_le(count as u64);
        buffer.put_u8(match dir {
            Direction::Forward => 0,
            Direction::Backward => 1,
        });

        buffer.freeze()
    }

    pub fn store(buffer: BytesMut, key: u64) -> StoreRequestBuilder {
        StoreRequestBuilder::new(buffer, key)
    }

    pub fn latest_revision(mut buffer: BytesMut, key: u64) -> Bytes {
        buffer.put_u8(0x02);
        buffer.put_u64_le(key);
        buffer.freeze()
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum IndexingResp {
    StreamDeleted,
    Committed,
    Streaming,
    Error,
}

impl IndexingResp {
    fn serialize(self, mut buffer: BytesMut) -> Bytes {
        match self {
            IndexingResp::StreamDeleted => buffer.put_u8(0x00),
            IndexingResp::Committed => buffer.put_u8(0x01),
            IndexingResp::Streaming => buffer.put_u8(0x02),
            IndexingResp::Error => buffer.put_u8(0x03),
        }

        buffer.split().freeze()
    }

    fn try_from(mut bytes: Bytes) -> eyre::Result<Self> {
        match bytes.get_u8() {
            0x00 => Ok(IndexingResp::StreamDeleted),
            0x01 => Ok(IndexingResp::Committed),
            0x02 => Ok(IndexingResp::Streaming),
            0x03 => Ok(IndexingResp::Error),
            _ => eyre::bail!("unknown response message from from the index process"),
        }
    }

    fn expect(self, expectation: IndexingResp) -> eyre::Result<()> {
        if self != expectation {
            eyre::bail!("expected {:?} but got {:?}", expectation, self);
        }

        Ok(())
    }
}

pub struct Indexing<S, WAL> {
    storage: S,
    wal: WALRef<WAL>,
    writer: Arc<AtomicU64>,
}

impl<S, WAL> Indexing<S, WAL> {
    pub fn new(storage: S, wal: WALRef<WAL>, writer: Arc<AtomicU64>) -> Self {
        Self {
            storage,
            wal,
            writer,
        }
    }
}

impl<S, WAL> RunnableRaw for Indexing<S, WAL>
where
    S: Storage + Send + Sync + 'static,
    WAL: WriteAheadLog + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        "indexing"
    }

    fn run(self: Box<Self>, mut env: ProcessRawEnv) -> eyre::Result<()> {
        let lsm = Lsm::load(LsmSettings::default(), self.storage.clone())?;
        let lsm = Arc::new(RwLock::new(lsm));
        let revision_cache = new_revision_cache();

        while let Some(item) = env.queue.recv().ok() {
            match item {
                Item::Mail(mail) => {
                    if let Some(req) = IndexingReq::try_from(mail.payload) {
                        match req {
                            IndexingReq::Store { key, entries } => {
                                let store_lsm = lsm.clone();
                                let store_client = env.client.clone();
                                let store_buffer = env.buffer.split();

                                Handle::current().spawn_blocking(move || {
                                    if let Err(e) = store_entries(store_lsm, key, entries) {
                                        tracing::error!("error when storing index entries: {}", e);

                                        let _ = store_client.reply(
                                            mail.origin,
                                            mail.correlation,
                                            IndexingResp::Error.serialize(store_buffer),
                                        );
                                    } else {
                                        let _ = store_client.reply(
                                            mail.origin,
                                            mail.correlation,
                                            IndexingResp::Committed.serialize(store_buffer),
                                        );
                                    }
                                });
                            }

                            IndexingReq::LatestRevision { key } => {
                                if let Some(current) = revision_cache.get(&key) {
                                    env.buffer.put_u64_le(current);
                                    env.client.reply(
                                        mail.origin,
                                        mail.correlation,
                                        env.buffer.split().freeze(),
                                    )?;
                                } else {
                                    let lsm_read = lsm.read().map_err(|e| {
                                        eyre::eyre!(
                                            "posoined lock when reading to the index: {}",
                                            e
                                        )
                                    })?;

                                    let revison = lsm_read.highest_revision(key)?;

                                    if let Some(revision) = revison {
                                        revision_cache.insert(key, revision);
                                        env.buffer.put_u64_le(revision);
                                    }

                                    env.client.reply(
                                        mail.origin,
                                        mail.correlation,
                                        env.buffer.split().freeze(),
                                    )?;
                                }
                            }

                            IndexingReq::Read { .. } => {
                                tracing::error!(
                                    "read from the index should be a streaming operation"
                                );

                                env.client.reply(
                                    mail.origin,
                                    mail.correlation,
                                    IndexingResp::Error.serialize(env.buffer.split()),
                                )?;
                            }
                        }
                    }
                }

                Item::Stream(stream) => {
                    if let Some(IndexingReq::Read {
                        key,
                        start,
                        count,
                        dir,
                    }) = IndexingReq::try_from(stream.payload)
                    {
                        let wal = self.wal.clone();
                        let stream_cache = revision_cache.clone();
                        let stream_lsm = lsm.clone();
                        let mut stream_buffer = env.buffer.split();
                        let _: JoinHandle<eyre::Result<()>> =
                            tokio::task::spawn_blocking(move || {
                                if stream_indexed_read(
                                    stream_lsm,
                                    wal,
                                    stream_cache,
                                    key,
                                    start,
                                    count as usize,
                                    dir,
                                    stream_buffer.split(),
                                    &stream.sender,
                                )
                                .is_err()
                                {
                                    let _ = stream
                                        .sender
                                        .send(IndexingResp::Error.serialize(stream_buffer.split()));
                                }

                                Ok(())
                            });
                    }
                }
            };
        }

        Ok(())
    }
}

fn key_latest_revision<S>(
    lsm: &Lsm<S>,
    cache: RevisionCache,
    stream_key: u64,
) -> io::Result<CurrentRevision>
where
    S: Storage + Send + Sync + 'static,
{
    let current_revision = if let Some(current) = cache.get(&stream_key) {
        CurrentRevision::Revision(current)
    } else {
        let revision = lsm
            .highest_revision(stream_key)?
            .map_or_else(|| CurrentRevision::NoStream, CurrentRevision::Revision);

        if let CurrentRevision::Revision(rev) = revision {
            cache.insert(stream_key, rev);
        }

        revision
    };

    Ok(current_revision)
}

fn store_entries<S>(lsm: Arc<RwLock<Lsm<S>>>, key: u64, entries: Bytes) -> eyre::Result<()>
where
    S: Storage + Send + Sync + 'static,
{
    let mut lsm = lsm
        .write()
        .map_err(|e| eyre::eyre!("posoined lock when writing to the index: {}", e))?;

    lsm.put_values(StoreEntries { key, entries })?;

    Ok(())
}

struct StoreEntries {
    key: u64,
    entries: Bytes,
}

impl Iterator for StoreEntries {
    type Item = (u64, u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.entries.has_remaining() {
            return None;
        }

        Some((
            self.key,
            self.entries.get_u64_le(),
            self.entries.get_u64_le(),
        ))
    }
}

fn stream_indexed_read<S, WAL>(
    lsm: Arc<RwLock<Lsm<S>>>,
    wal: WALRef<WAL>,
    cache: RevisionCache,
    key: u64,
    start: u64,
    count: usize,
    dir: Direction,
    mut buffer: BytesMut,
    stream: &UnboundedSender<Bytes>,
) -> eyre::Result<()>
where
    S: Storage + Send + Sync + 'static,
    WAL: WriteAheadLog + Send + Sync + 'static,
{
    let lsm = lsm
        .read()
        .map_err(|e| eyre::eyre!("posoined lock when reading the index: {}", e))?;

    let current_revision = key_latest_revision(&lsm, cache, key)?;

    if current_revision.is_deleted() {
        if stream
            .send(IndexingResp::StreamDeleted.serialize(buffer.split()))
            .is_err()
        {
            return Ok(());
        }
    }

    let mut iter: Box<dyn IteratorIO<Item = BlockEntry>> = match dir {
        Direction::Forward => Box::new(lsm.scan_forward(key, start, count)),
        Direction::Backward => Box::new(lsm.scan_backward(key, start, count)),
    };

    while let Some(item) = iter.next()? {
        let record = wal.read_at(item.position)?;

        if stream.send(record.payload).is_err() {
            break;
        }
    }

    Ok(())
}
