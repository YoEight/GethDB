use super::{Item, Mail, ManagerClient, ProcessEnv, Runnable};
use crate::domain::index::{new_revision_cache, CurrentRevision};
use crate::process::indexing::chaser::Chaser;
use bytes::{Buf, Bytes, BytesMut};
use geth_domain::{Lsm, LsmSettings};
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};
use std::io;
use std::io::BufRead;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::RwLock;

mod chaser;
mod committer;

type RevisionCache = moka::sync::Cache<u64, u64>;
enum IndexingReq {
    Read { key: u64, start: u64, count: u64 },
}

enum IndexingResp {
    StreamDeleted,
}

impl IndexingResp {
    fn serialize(&self, buffer: &mut BytesMut) -> Bytes {
        buffer.split().freeze()
    }
}

impl IndexingReq {
    fn try_from(mut bytes: Bytes) -> Option<Self> {
        match bytes.get_u8() {
            0x00 => Some(Self::Read {
                key: bytes.get_u64_le(),
                start: bytes.get_u64_le(),
                count: bytes.get_u64_le(),
            }),
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
                        IndexingReq::Read { key, start, count } => {
                            let client = env.client.clone();
                            let wal = self.wal.clone();
                            let stream_cache = revision_cache.clone();

                            tokio::task::spawn_blocking(move || {});
                        }
                    }
                }
            }
        }
    }
}

pub fn current_revision<S>(
    lsm: Lsm<S>,
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
