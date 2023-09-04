use chrono::{TimeZone, Utc};
use std::io;
use tokio::task::spawn_blocking;

use geth_common::Position;
use geth_mikoshi::domain::StreamEventAppended;
use geth_mikoshi::wal::{LogEntryType, WALRef, WriteAheadLog};
use geth_mikoshi::{
    hashing::mikoshi_hash, index::IteratorIO, storage::Storage, Entry, MikoshiStream,
};

use crate::messages::{ReadStream, ReadStreamCompleted};
use crate::process::storage::index::StorageIndex;

#[derive(Clone)]
pub struct StorageReader<WAL, S>
where
    S: Storage,
{
    wal: WALRef<WAL>,
    index: StorageIndex<S>,
}

impl<WAL, S> StorageReader<WAL, S>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    pub fn new(wal: WALRef<WAL>, index: StorageIndex<S>) -> Self {
        Self { wal, index }
    }

    pub async fn read(&self, params: ReadStream) -> ReadStreamCompleted {
        let index = self.index.clone();
        let wal = self.wal.clone();
        let (send_result, recv_result) = tokio::sync::oneshot::channel();

        spawn_blocking(move || {
            let current_revision = index.stream_current_revision(&params.stream_name)?;

            if current_revision.is_deleted() {
                let _ = send_result.send(ReadStreamCompleted::StreamDeleted);
                return Ok::<_, io::Error>(());
            }

            let mut iter = index.lsm().scan(
                mikoshi_hash(&params.stream_name),
                params.direction,
                params.starting,
                params.count,
            );

            let (read_stream, read_queue) = tokio::sync::mpsc::channel(500);
            let stream = MikoshiStream::new(read_queue);
            let _ = send_result.send(ReadStreamCompleted::Success(stream));

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

            Ok(())
        });

        match recv_result.await {
            Err(_) => ReadStreamCompleted::Unexpected(eyre::eyre!("I/O operation became unbound")),
            Ok(r) => r,
        }
    }
}
