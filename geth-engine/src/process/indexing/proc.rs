use crate::domain::index::CurrentRevision;
use crate::process::messages::{IndexRequests, IndexResponses, Messages};
use crate::process::{Item, ProcessRawEnv, Runtime};
use bytes::{Buf, Bytes};
use geth_common::{Direction, IteratorIO};
use geth_domain::index::BlockEntry;
use geth_domain::{Lsm, LsmSettings};
use geth_mikoshi::storage::Storage;
use std::cmp::min;
use std::sync::{Arc, RwLock};
use std::{io, mem};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

type RevisionCache = moka::sync::Cache<u64, u64>;

fn new_revision_cache() -> RevisionCache {
    moka::sync::Cache::<u64, u64>::builder()
        .max_capacity(10_000)
        .name("revision-cache")
        .build()
}

pub fn run<S>(runtime: Runtime<S>, mut env: ProcessRawEnv) -> eyre::Result<()>
where
    S: Storage + Send + Sync + 'static,
{
    let lsm = Lsm::load(
        LsmSettings::default(),
        runtime.container().storage().clone(),
    )?;

    let lsm = Arc::new(RwLock::new(lsm));
    let revision_cache = new_revision_cache();

    while let Some(item) = env.queue.recv().ok() {
        match item {
            Item::Mail(mail) => {
                if let Some(req) = mail.payload.try_into().ok() {
                    match req {
                        IndexRequests::Store { entries } => {
                            let last = entries.last().copied().unwrap();
                            if let Err(e) = store_entries(&lsm, entries) {
                                tracing::error!("error when storing index entries: {}", e);

                                let _ = env.client.reply(
                                    mail.origin,
                                    mail.correlation,
                                    IndexResponses::Error.into(),
                                );
                            } else {
                                revision_cache.insert(last.key, last.revision);

                                let _ = env.client.reply(
                                    mail.origin,
                                    mail.correlation,
                                    IndexResponses::Committed.into(),
                                );
                            }
                        }

                        IndexRequests::LatestRevision { key } => {
                            if let Some(current) = revision_cache.get(&key) {
                                env.client.reply(
                                    mail.origin,
                                    mail.correlation,
                                    IndexResponses::CurrentRevision(CurrentRevision::Revision(
                                        current,
                                    ))
                                    .into(),
                                )?;
                            } else {
                                let lsm_read = lsm.read().map_err(|e| {
                                    eyre::eyre!("poisoned lock when reading to the index: {}", e)
                                })?;

                                let revison = lsm_read.highest_revision(key)?;
                                let mut value = CurrentRevision::NoStream;

                                if let Some(revision) = revison {
                                    revision_cache.insert(key, revision);
                                    value = CurrentRevision::Revision(revision);
                                }

                                env.client.reply(
                                    mail.origin,
                                    mail.correlation,
                                    IndexResponses::CurrentRevision(value).into(),
                                )?;
                            }
                        }

                        IndexRequests::Read { .. } => {
                            tracing::error!("read from the index should be a streaming operation");

                            env.client.reply(
                                mail.origin,
                                mail.correlation,
                                IndexResponses::Error.into(),
                            )?;
                        }
                    }
                }
            }

            Item::Stream(stream) => {
                if let Some(IndexRequests::Read {
                    key,
                    start,
                    count,
                    dir,
                }) = stream.payload.try_into().ok()
                {
                    let stream_cache = revision_cache.clone();
                    let stream_lsm = lsm.clone();
                    let _: JoinHandle<eyre::Result<()>> = env.handle.spawn_blocking(move || {
                        if stream_indexed_read(
                            stream_lsm,
                            stream_cache,
                            key,
                            start,
                            count as usize,
                            dir,
                            &stream.sender,
                        )
                        .is_err()
                        {
                            let _ = stream.sender.send(IndexResponses::Error.into());
                        }

                        Ok(())
                    });
                }
            }
        };
    }

    Ok(())
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

fn store_entries<S>(lsm: &Arc<RwLock<Lsm<S>>>, entries: Vec<BlockEntry>) -> eyre::Result<()>
where
    S: Storage + Send + Sync + 'static,
{
    let mut lsm = lsm
        .write()
        .map_err(|e| eyre::eyre!("poisoned lock when writing to the index: {}", e))?;

    lsm.put_values(entries.into_iter().map(|e| (e.key, e.revision, e.position)))?;

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
    stream: &UnboundedSender<Messages>,
) -> eyre::Result<()>
where
    S: Storage + Send + Sync + 'static,
{
    let lsm = lsm
        .read()
        .map_err(|e| eyre::eyre!("poisoned lock when reading the index: {}", e))?;

    let current_revision = key_latest_revision(&lsm, cache, key)?;

    if current_revision.is_deleted() {
        if stream.send(IndexResponses::StreamDeleted.into()).is_err() {
            return Ok(());
        }
    }

    let mut iter: Box<dyn IteratorIO<Item = BlockEntry>> = match dir {
        Direction::Forward => Box::new(lsm.scan_forward(key, start, count)),
        Direction::Backward => Box::new(lsm.scan_backward(key, start, count)),
    };

    let batch_size = min(count, 500);
    let mut batch = Vec::with_capacity(batch_size);
    while let Some(item) = iter.next()? {
        if batch.len() >= batch_size {
            let entries = mem::replace(&mut batch, Vec::with_capacity(batch_size));
            if stream
                .send(IndexResponses::Entries(entries).into())
                .is_err()
            {
                return Ok(());
            }
        }

        batch.push(item);
    }

    if !batch.is_empty() {
        let _ = stream.send(IndexResponses::Entries(batch).into());
    }

    Ok(())
}
