use super::{Item, ProcessEnv, Runnable};
use crate::domain::index::CurrentRevision;
use crate::process::indexing::chaser::Chaser;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use geth_common::{Direction, IteratorIO};
use geth_domain::index::BlockEntry;
use geth_domain::{Lsm, LsmSettings};
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};
use std::io;
use std::io::BufRead;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;

mod chaser;
mod committer;

type RevisionCache = moka::sync::Cache<u64, u64>;

pub fn new_revision_cache() -> RevisionCache {
    moka::sync::Cache::<u64, u64>::builder()
        .max_capacity(10_000)
        .name("revision-cache")
        .build()
}

enum IndexingReq {
    Read {
        key: u64,
        start: u64,
        count: u64,
        dir: Direction,
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
            _ => unreachable!(),
        }
    }

    pub fn serialize(self, mut buffer: BytesMut) -> Bytes {
        match self {
            IndexingReq::Read {
                key,
                start,
                count,
                dir,
            } => {
                buffer.put_u64_le(key);
                buffer.put_u64_le(start);
                buffer.put_u64_le(count);
                buffer.put_u8(match dir {
                    Direction::Forward => 0,
                    Direction::Backward => 1,
                });
            }
        }

        buffer.split().freeze()
    }
}

pub enum IndexingResp {
    StreamDeleted,
}

impl IndexingResp {
    fn serialize(self, mut buffer: BytesMut) -> Bytes {
        match self {
            IndexingResp::StreamDeleted => buffer.put_u8(0x00),
        }

        buffer.split().freeze()
    }

    fn deserialize(mut bytes: Bytes) -> Self {
        match bytes.get_u8() {
            0x00 => IndexingResp::StreamDeleted,
            _ => unreachable!(),
        }
    }
}

pub struct Indexing<S, WAL> {
    storage: S,
    wal: WALRef<WAL>,
}

#[async_trait::async_trait]
impl<S, WAL> Runnable for Indexing<S, WAL>
where
    S: Storage + Send + Sync + 'static,
    WAL: WriteAheadLog + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        "indexing"
    }

    async fn run(self: Box<Self>, mut env: ProcessEnv) {
        let chase_chk = Arc::new(AtomicU64::new(0));
        let lsm_storage = self.storage.clone();
        // TODO - implement reporting to the manager when the indexing process crashed.
        let lsm =
            tokio::task::spawn_blocking(move || Lsm::load(LsmSettings::default(), lsm_storage))
                .await
                .unwrap()
                .unwrap();

        let lsm = Arc::new(RwLock::new(lsm));
        let revision_cache = new_revision_cache();

        env.client.spawn(Chaser::new(chase_chk, lsm.clone()));

        while let Some(item) = env.queue.recv().await {
            if let Item::Mail(mail) = item {
                if let Some(req) = IndexingReq::try_from(mail.payload) {
                    match req {
                        IndexingReq::Read {
                            key,
                            start,
                            count,
                            dir,
                        } => {
                            let client = env.client.clone();
                            let wal = self.wal.clone();
                            let stream_cache = revision_cache.clone();
                            let stream_lsm = lsm.clone();
                            let stream_buffer = env.buffer.split();

                            let _: JoinHandle<io::Result<()>> =
                                tokio::task::spawn_blocking(move || {
                                    let lsm = stream_lsm.read().unwrap();
                                    let current_revision =
                                        key_latest_revision(&lsm, stream_cache, key)?;

                                    if current_revision.is_deleted() {
                                        client.reply(
                                            mail.origin,
                                            mail.correlation,
                                            IndexingResp::StreamDeleted.serialize(stream_buffer),
                                        );

                                        return Ok(());
                                    }

                                    let mut iter: Box<dyn IteratorIO<Item = BlockEntry>> = match dir
                                    {
                                        Direction::Forward => {
                                            Box::new(lsm.scan_forward(key, start, count as usize))
                                        }
                                        Direction::Backward => {
                                            Box::new(lsm.scan_backward(key, start, count as usize))
                                        }
                                    };

                                    while let Some(item) = iter.next()? {
                                        let record = wal.read_at(item.position)?;
                                        client.reply(mail.origin, mail.correlation, record.payload);
                                    }

                                    Ok(())
                                });
                        }
                    }
                }
            }
        }
    }
}

pub fn key_latest_revision<S>(
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
