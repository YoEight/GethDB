use chrono::{TimeZone, Utc};
use std::io;
use std::sync::mpsc;

use geth_common::Position;
use geth_mikoshi::domain::StreamEventAppended;
use geth_mikoshi::wal::{LogEntryType, WALRef, WriteAheadLog};
use geth_mikoshi::{
    hashing::mikoshi_hash,
    index::{IteratorIO, Lsm},
    storage::Storage,
    Entry, MikoshiStream,
};

use crate::bus::ReadStreamMsg;
use crate::messages::{ReadStream, ReadStreamCompleted};
use crate::process::storage::service::current::CurrentRevision;
use crate::process::storage::RevisionCache;

pub struct StorageReaderService<WAL, S> {
    wal: WALRef<WAL>,
    index: Lsm<S>,
    revision_cache: RevisionCache,
}

impl<WAL, S> StorageReaderService<WAL, S>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    pub fn new(wal: WALRef<WAL>, index: Lsm<S>, revision_cache: RevisionCache) -> Self {
        Self {
            wal,
            index,
            revision_cache,
        }
    }

    pub fn read(&mut self, params: ReadStream) -> io::Result<ReadStreamCompleted> {
        let key = mikoshi_hash(&params.stream_name);
        let current_revision = if let Some(current) = self.revision_cache.get(&params.stream_name) {
            CurrentRevision::Revision(current)
        } else {
            self.index
                .highest_revision(key)?
                .map_or_else(|| CurrentRevision::NoStream, CurrentRevision::Revision)
        };

        if current_revision.is_deleted() {
            return Ok(ReadStreamCompleted::StreamDeleted);
        }

        let mut iter = self
            .index
            .scan(key, params.direction, params.starting, params.count);

        let wal = self.wal.clone();
        let (read_stream, read_queue) = tokio::sync::mpsc::channel(500);
        let stream = MikoshiStream::new(read_queue);

        tokio::task::spawn_blocking(move || {
            while let Some(entry) = iter.next()? {
                let record = wal.read_at(entry.position)?;
                if record.r#type != LogEntryType::UserData {
                    continue;
                }

                let event = record.unmarshall::<StreamEventAppended>();
                let entry = Entry {
                    id: event.event_id,
                    r#type: event.event_type,
                    stream_name: event.event_stream_id,
                    revision: event.revision,
                    data: event.data,
                    position: Position(record.position),
                    created: Utc.timestamp_opt(event.created, 0).unwrap(),
                };

                // if failing means that we don't need to read form the transaction log.
                if read_stream.blocking_send(entry).is_err() {
                    break;
                }
            }

            Ok::<_, io::Error>(())
        });

        Ok(ReadStreamCompleted::Success(stream))
    }
}

pub fn start<WAL, S>(
    wal: WALRef<WAL>,
    index: Lsm<S>,
    revision_cache: RevisionCache,
) -> mpsc::Sender<ReadStreamMsg>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let (sender, recv) = mpsc::channel();
    let service = StorageReaderService::new(wal, index, revision_cache);

    tokio::task::spawn_blocking(|| process(service, recv));

    sender
}

fn process<WAL, S>(
    mut service: StorageReaderService<WAL, S>,
    queue: mpsc::Receiver<ReadStreamMsg>,
) -> io::Result<()>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    while let Ok(msg) = queue.recv() {
        let stream_name = msg.payload.stream_name.clone();
        match service.read(msg.payload) {
            Err(e) => {
                let _ = msg.mail.send(Err(eyre::eyre!(
                    "Error when reading from '{}': {}",
                    stream_name,
                    e
                )));
            }
            Ok(result) => {
                let _ = msg.mail.send(Ok(result));
            }
        }
    }

    Ok(())
}
