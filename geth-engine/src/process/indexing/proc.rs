use crate::domain::index::CurrentRevision;
use crate::process::indexing::{IndexingReq, IndexingResp, ENTRY_SIZE};
use crate::process::{Item, ProcessRawEnv, RunnableRaw};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use geth_common::{Direction, IteratorIO};
use geth_domain::index::BlockEntry;
use geth_domain::{Lsm, LsmSettings};
use geth_mikoshi::storage::Storage;
use std::cmp::min;
use std::io;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

type RevisionCache = moka::sync::Cache<u64, u64>;

fn new_revision_cache() -> RevisionCache {
    moka::sync::Cache::<u64, u64>::builder()
        .max_capacity(10_000)
        .name("revision-cache")
        .build()
}

pub struct Indexing<S> {
    storage: S,
}

impl<S> Indexing<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

impl<S> RunnableRaw for Indexing<S>
where
    S: Storage + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        "index"
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
                                let last_revision = entries
                                    .slice(entries.len() - 2 * std::mem::size_of::<u64>()..)
                                    .get_u64_le();

                                if let Err(e) = store_entries(&lsm, key, entries) {
                                    tracing::error!("error when storing index entries: {}", e);

                                    let _ = env.client.reply(
                                        mail.origin,
                                        mail.correlation,
                                        IndexingResp::Error.serialize(env.buffer.split()),
                                    );
                                } else {
                                    revision_cache.insert(key, last_revision);

                                    let _ = env.client.reply(
                                        mail.origin,
                                        mail.correlation,
                                        IndexingResp::Committed.serialize(env.buffer.split()),
                                    );
                                }
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
                                            "poisoned lock when reading to the index: {}",
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
                        let stream_cache = revision_cache.clone();
                        let stream_lsm = lsm.clone();
                        let mut stream_buffer = env.buffer.split();
                        let _: JoinHandle<eyre::Result<()>> =
                            env.handle.spawn_blocking(move || {
                                if stream_indexed_read(
                                    stream_lsm,
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

fn store_entries<S>(lsm: &Arc<RwLock<Lsm<S>>>, key: u64, entries: Bytes) -> eyre::Result<()>
where
    S: Storage + Send + Sync + 'static,
{
    let mut lsm = lsm
        .write()
        .map_err(|e| eyre::eyre!("poisoned lock when writing to the index: {}", e))?;

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

fn stream_indexed_read<S>(
    lsm: Arc<RwLock<Lsm<S>>>,
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
{
    let lsm = lsm
        .read()
        .map_err(|e| eyre::eyre!("poisoned lock when reading the index: {}", e))?;

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

    if stream
        .send(IndexingResp::Streaming.serialize(buffer.split()))
        .is_err()
    {
        return Ok(());
    }

    let batch_size = min(count, 500);
    let mem_requirement = batch_size * ENTRY_SIZE;
    if buffer.remaining() < mem_requirement {
        buffer.reserve(mem_requirement - buffer.remaining());
    }

    let mut count = 0;
    while let Some(item) = iter.next()? {
        if count >= batch_size {
            count = 0;
            if stream.send(buffer.split().freeze()).is_err() {
                return Ok(());
            }
        }

        count += 1;
        buffer.put_u64_le(item.revision);
        buffer.put_u64_le(item.position);
    }

    if !buffer.is_empty() {
        let _ = stream.send(buffer.split().freeze());
    }

    Ok(())
}
