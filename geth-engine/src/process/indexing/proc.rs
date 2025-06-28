use crate::domain::index::CurrentRevision;
use crate::names::types::STREAM_DELETED;
use crate::process::messages::{IndexRequests, IndexResponses, Messages};
use crate::process::reading::record_try_from;
use crate::process::{Item, ProcessEnv, Raw, RequestContext};
use crate::{get_chunk_container, get_storage};
use geth_common::{Direction, IteratorIO};
use geth_domain::index::BlockEntry;
use geth_domain::{Lsm, LsmSettings};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::wal::chunks::ChunkContainer;
use geth_mikoshi::wal::LogReader;
use std::cmp::min;
use std::sync::{Arc, RwLock};
use std::{io, mem};
use tokio::sync::mpsc::UnboundedSender;
use tracing::instrument;
use uuid::Uuid;

type RevisionCache = moka::sync::Cache<u64, u64>;

fn new_revision_cache() -> RevisionCache {
    moka::sync::Cache::<u64, u64>::builder()
        .max_capacity(10_000)
        .name(&format!("revision-cache-{}", Uuid::new_v4()))
        .build()
}

#[instrument(skip(env), fields(origin = ?env.proc))]
pub fn run(mut env: ProcessEnv<Raw>) -> eyre::Result<()> {
    let mut lsm = Lsm::load(LsmSettings::default(), get_storage())?;

    tracing::info!("rebuilding index...");
    let revision_cache = rebuild_index(&mut lsm, get_chunk_container().clone())?;
    tracing::info!("index rebuilt successfully");

    let lsm = Arc::new(RwLock::new(lsm));

    while let Some(item) = env.recv() {
        match item {
            Item::Mail(mail) => {
                if let Ok(req) = mail.payload.try_into() {
                    match req {
                        IndexRequests::Store { entries } => {
                            if entries.is_empty() {
                                tracing::warn!("empty entries vector received");

                                let _ = env.client.reply(
                                    mail.context,
                                    mail.origin,
                                    mail.correlation,
                                    IndexResponses::Committed.into(),
                                );

                                continue;
                            }

                            let last = entries.last().copied().unwrap();
                            if let Err(e) = store_entries(&lsm, entries) {
                                tracing::error!("error when storing index entries: {}", e);

                                let _ = env.client.reply(
                                    mail.context,
                                    mail.origin,
                                    mail.correlation,
                                    IndexResponses::Error.into(),
                                );
                            } else {
                                revision_cache.insert(last.key, last.revision);

                                let _ = env.client.reply(
                                    mail.context,
                                    mail.origin,
                                    mail.correlation,
                                    IndexResponses::Committed.into(),
                                );
                            }
                        }

                        IndexRequests::LatestRevision { key } => {
                            if let Some(current) = revision_cache.get(&key) {
                                env.client.reply(
                                    mail.context,
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
                                    mail.context,
                                    mail.origin,
                                    mail.correlation,
                                    IndexResponses::CurrentRevision(value).into(),
                                )?;
                            }
                        }

                        IndexRequests::Read { .. } => {
                            tracing::error!("read from the index should be a streaming operation");

                            env.client.reply(
                                mail.context,
                                mail.origin,
                                mail.correlation,
                                IndexResponses::Error.into(),
                            )?;
                        }
                    }
                }
            }

            Item::Stream(stream) => {
                if let Ok(IndexRequests::Read {
                    key,
                    start,
                    count,
                    dir,
                }) = stream.payload.try_into()
                {
                    let stream_cache = revision_cache.clone();
                    let stream_lsm = lsm.clone();
                    env.spawn_blocking(move || {
                        if let Err(error) = stream_indexed_read(IndexRead {
                            context: stream.context,
                            lsm: stream_lsm,
                            cache: stream_cache,
                            key,
                            start,
                            count,
                            dir,
                            stream: &stream.sender,
                        }) {
                            tracing::error!(%error, "error when reading the index");
                            let _ = stream.sender.send(IndexResponses::Error.into());
                        }
                    });
                }
            }
        };
    }

    Ok(())
}

fn rebuild_index(lsm: &mut Lsm, container: ChunkContainer) -> eyre::Result<RevisionCache> {
    let reader = LogReader::new(container);
    let writer_checkpoint = reader.get_writer_checkpoint()?;
    let cache = new_revision_cache();
    let mut entries = reader.entries(0, writer_checkpoint);

    while let Some(entry) = entries.next()? {
        if entry.r#type != 0 {
            continue;
        }

        let record = record_try_from(entry)?;
        let key = mikoshi_hash(&record.stream_name);

        let final_revision = if record.class == STREAM_DELETED {
            u64::MAX
        } else {
            record.revision
        };

        lsm.put_single(key, final_revision, record.position)?;
        cache.insert(key, record.revision);
    }

    Ok(cache)
}

fn key_latest_revision(
    lsm: &Lsm,
    cache: RevisionCache,
    stream_key: u64,
) -> io::Result<CurrentRevision> {
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

fn store_entries(lsm: &Arc<RwLock<Lsm>>, entries: Vec<BlockEntry>) -> eyre::Result<()> {
    let mut lsm = lsm
        .write()
        .map_err(|e| eyre::eyre!("poisoned lock when writing to the index: {}", e))?;

    lsm.put_values(entries.into_iter().map(|e| (e.key, e.revision, e.position)))?;

    Ok(())
}

struct IndexRead<'a> {
    context: RequestContext,
    lsm: Arc<RwLock<Lsm>>,
    cache: RevisionCache,
    key: u64,
    start: u64,
    count: usize,
    dir: Direction,
    stream: &'a UnboundedSender<Messages>,
}

#[instrument(skip(params), fields(correlation = %params.context.correlation, key = params.key, start = params.start, count = params.count, direction = ?params.dir))]
fn stream_indexed_read(params: IndexRead<'_>) -> eyre::Result<()> {
    let lsm = params
        .lsm
        .read()
        .map_err(|e| eyre::eyre!("poisoned lock when reading the index: {}", e))?;

    let current_revision = key_latest_revision(&lsm, params.cache, params.key)?;

    if current_revision.is_deleted()
        && params
            .stream
            .send(IndexResponses::StreamDeleted.into())
            .is_err()
    {
        return Ok(());
    }

    let mut iter: Box<dyn IteratorIO<Item = BlockEntry>> = match params.dir {
        Direction::Forward => Box::new(lsm.scan_forward(params.key, params.start, params.count)),
        Direction::Backward => Box::new(lsm.scan_backward(params.key, params.start, params.count)),
    };

    let batch_size = min(params.count, 500);
    let mut batch = Vec::with_capacity(batch_size);
    let mut no_entries = true;
    while let Some(item) = iter.next()? {
        if batch.len() >= batch_size {
            let entries = mem::replace(&mut batch, Vec::with_capacity(batch_size));
            if params
                .stream
                .send(IndexResponses::Entries(entries).into())
                .is_err()
            {
                return Ok(());
            }
        }

        batch.push(item);
        no_entries = false;
    }

    if !batch.is_empty() {
        let _ = params.stream.send(IndexResponses::Entries(batch).into());
        return Ok(());
    }

    if no_entries {
        let _ = params
            .stream
            .send(IndexResponses::Entries(Vec::new()).into());
    }

    Ok(())
}
