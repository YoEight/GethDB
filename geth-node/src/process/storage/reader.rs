use std::io;

use tokio::task::spawn_blocking;

use geth_common::{IteratorIO, Position};
use geth_domain::binary::events::Event;
use geth_domain::RecordedEvent;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};
use geth_mikoshi::{hashing::mikoshi_hash, storage::Storage, Entry, MikoshiStream};

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

                let event = if let Ok(event) = geth_domain::parse_event(record.payload.as_ref()) {
                    if let Event::RecordedEvent(event) = event.event.unwrap() {
                        RecordedEvent::from(event)
                    } else {
                        panic!("We expected a record event at that log position");
                    }
                } else {
                    panic!("Error when parsing command from the transaction log");
                };

                let entry = Entry {
                    id: event.id,
                    r#type: event.class,
                    stream_name: event.stream_name,
                    revision: event.revision,
                    data: event.data.into(),
                    position: Position(record.position),
                    created: event.created,
                };

                // if failing means that we don't need to read form the transaction log.
                if read_stream.blocking_send(entry).is_err() {
                    break;
                }
            }

            Ok(())
        });

        recv_result.await.unwrap_or_else(|_| {
            ReadStreamCompleted::Unexpected(eyre::eyre!("I/O operation became unbound"))
        })
    }
}
