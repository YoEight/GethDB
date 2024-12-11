use crate::process::indexing::IndexingResp;
use crate::process::{Item, ProcessRawEnv, RunnableRaw};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use geth_common::IteratorIO;
use geth_domain::binary::models::{Event, Events};
use geth_domain::Lsm;
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::storage::{Checkpoint, FileId, Storage};
use geth_mikoshi::wal::{WALRef, WriteAheadLog};
use prost::Message;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use uuid::Uuid;

pub struct Chaser<S, WAL> {
    chk: Arc<AtomicU64>,
    writer_chk: Arc<AtomicU64>,
    lsm: Arc<RwLock<Lsm<S>>>,
    wal: WALRef<WAL>,
}

pub struct Chasing {
    origin: Uuid,
    correlation: Uuid,
    position: u64,
}

impl Chasing {
    fn from(mut bytes: Bytes) -> Self {
        Self {
            origin: Uuid::from_u128_le(bytes.get_u128_le()),
            correlation: Uuid::from_u128_le(bytes.get_u128_le()),
            position: bytes.get_u64_le(),
        }
    }

    pub fn new(origin: Uuid, correlation: Uuid, position: u64) -> Self {
        Self {
            origin,
            correlation,
            position,
        }
    }

    pub fn serialize(self, mut buffer: BytesMut) -> Bytes {
        buffer.put_u128_le(self.origin.to_u128_le());
        buffer.put_u128_le(self.correlation.to_u128_le());
        buffer.put_u64_le(self.position);
        buffer.freeze()
    }
}

impl<S, WAL> Chaser<S, WAL> {
    pub fn new(
        chk: Arc<AtomicU64>,
        writer_chk: Arc<AtomicU64>,
        lsm: Arc<RwLock<Lsm<S>>>,
        wal: WALRef<WAL>,
    ) -> Self {
        Self {
            chk,
            writer_chk,
            lsm,
            wal,
        }
    }
}

impl<S, WAL> RunnableRaw for Chaser<S, WAL>
where
    S: Storage + Sync + Send + 'static,
    WAL: WriteAheadLog + Sync + Send + 'static,
{
    fn name(&self) -> &'static str {
        "chaser"
    }

    fn run(self: Box<Self>, mut env: ProcessRawEnv) -> eyre::Result<()> {
        let mut target = None;
        let mut chase_chk = self.chk.load(Ordering::Acquire);

        loop {
            match env.queue.recv_timeout(Duration::from_millis(100)) {
                Err(e) => {
                    if let RecvTimeoutError::Disconnected = e {
                        return Ok(());
                    }

                    let writer_chk = self.writer_chk.load(Ordering::Acquire);

                    if chase_chk == writer_chk {
                        continue;
                    }
                }

                Ok(item) => {
                    if let Item::Mail(mail) = item {
                        let msg = Chasing::from(mail.payload);

                        if chase_chk >= msg.position {
                            env.client.reply(
                                msg.origin,
                                msg.correlation,
                                IndexingResp::Committed.serialize(env.buffer.split()),
                            );

                            continue;
                        }

                        target = Some((msg.origin, msg.correlation));
                    } else {
                        continue;
                    }
                }
            };

            let entries = self.wal.entries(chase_chk);
            let mut lsm = self.lsm.write().unwrap();
            let values = entries.map(|entry| {
                chase_chk += entry.position;

                match Events::decode(entry.payload) {
                    Err(e) => {
                        panic!("cannot decode recorded events: {}", e);
                    }

                    Ok(record) => match record.event.unwrap() {
                        Event::RecordedEvent(event) => (
                            mikoshi_hash(&event.stream_name),
                            event.revision,
                            entry.position,
                        ),
                        Event::StreamDeleted(deleted) => {
                            (mikoshi_hash(&deleted.stream_name), u64::MAX, entry.position)
                        }
                    },
                }
            });

            lsm.put(values)?;

            if let Some((origin, correlation)) = target.take() {
                env.client.reply(
                    origin,
                    correlation,
                    IndexingResp::Committed.serialize(env.buffer.split()),
                );
            }

            let mut buffer = env.buffer.split();
            buffer.put_u64_le(chase_chk);
            lsm.storage()
                .write_to(FileId::Checkpoint(Checkpoint::Index), 0, buffer.freeze())?;

            self.chk.store(chase_chk, Ordering::Release);
        }
    }
}
