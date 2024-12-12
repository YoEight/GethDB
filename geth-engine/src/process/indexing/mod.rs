use super::{Item, ManagerClient, ProcessEnv, ProcessRawEnv, RunnableRaw};
use crate::domain::index::CurrentRevision;
use crate::process::indexing::chaser::{Chaser, Chasing};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::SinkExt;
use geth_common::{Direction, IteratorIO};
use geth_domain::index::BlockEntry;
use geth_domain::{Lsm, LsmSettings};
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use uuid::Uuid;

mod chaser;
mod committer;

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

    pub async fn chase(&mut self, position: u64) -> eyre::Result<()> {
        let payload = IndexingReq::Chase { position }.serialize(self.buffer.split());
        let resp = self.inner.request(self.target, payload).await?;

        IndexingResp::try_from(resp.payload)?.expect(IndexingResp::Committed)
    }

    pub async fn read(
        &mut self,
        key: u64,
        start: u64,
        count: usize,
        dir: Direction,
    ) -> eyre::Result<Streaming> {
        let payload = IndexingReq::Read {
            key,
            start,
            count: count as u64,
            dir,
        }
        .serialize(self.buffer.split());

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
}

enum IndexingReq {
    Read {
        key: u64,
        start: u64,
        count: u64,
        dir: Direction,
    },

    Chase {
        position: u64,
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

            0x01 => Some(Self::Chase {
                position: bytes.get_u64_le(),
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
                buffer.put_u8(0x00);
                buffer.put_u64_le(key);
                buffer.put_u64_le(start);
                buffer.put_u64_le(count);
                buffer.put_u8(match dir {
                    Direction::Forward => 0,
                    Direction::Backward => 1,
                });
            }

            IndexingReq::Chase { position } => {
                buffer.put_u8(0x01);
                buffer.put_u64_le(position);
            }
        }

        buffer.split().freeze()
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

impl<S, WAL> RunnableRaw for Indexing<S, WAL>
where
    S: Storage + Send + Sync + 'static,
    WAL: WriteAheadLog + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        "indexing"
    }

    fn run(self: Box<Self>, mut env: ProcessRawEnv) -> eyre::Result<()> {
        let chase_chk = Arc::new(AtomicU64::new(0));
        let lsm = Lsm::load(LsmSettings::default(), self.storage.clone())?;
        let lsm = Arc::new(RwLock::new(lsm));
        let revision_cache = new_revision_cache();

        let chaser_proc_id = env.handle.block_on(env.client.spawn_raw(Chaser::new(
            chase_chk.clone(),
            self.writer.clone(),
            lsm.clone(),
            self.wal.clone(),
        )))?;

        while let Some(item) = env.queue.recv().ok() {
            match item {
                Item::Mail(mail) => {
                    if let Some(IndexingReq::Chase { position }) =
                        IndexingReq::try_from(mail.payload)
                    {
                        // If we already know that the chaser process covered passed that
                        // position, we can immediately return that data was committed to the
                        // origin of the message.
                        if chase_chk.load(Ordering::Acquire) >= position {
                            env.client.reply(
                                mail.origin,
                                mail.correlation,
                                IndexingResp::Committed.serialize(env.buffer.split()),
                            )?;

                            continue;
                        }

                        env.client.send(
                            chaser_proc_id,
                            Chasing::new(mail.origin, mail.correlation, position)
                                .serialize(env.buffer.split()),
                        )?;
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
