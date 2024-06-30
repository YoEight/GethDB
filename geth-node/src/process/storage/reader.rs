use std::io;

use chrono::{TimeZone, Utc};
use tokio::sync::oneshot;
use tokio::task::spawn_blocking;
use uuid::Uuid;

use geth_common::{IteratorIO, Position};
use geth_domain::binary::events::Event;
use geth_domain::RecordedEvent;
use geth_mikoshi::{Entry, hashing::mikoshi_hash, MikoshiStream, storage::Storage};
use geth_mikoshi::wal::{WALRef, WriteAheadLog};

use crate::messages::{ReadStream, ReadStreamCompleted};
use crate::names;
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
            if params.stream_name == names::streams::ALL {
                direct_read(wal, send_result, params)
            } else {
                indexed_read(index, wal, send_result, params)
            }
        });

        recv_result.await.unwrap_or_else(|_| {
            ReadStreamCompleted::Unexpected(eyre::eyre!("I/O operation became unbound"))
        })
    }
}

fn indexed_read<WAL, S>(
    index: StorageIndex<S>,
    wal: WALRef<WAL>,
    send_result: oneshot::Sender<ReadStreamCompleted>,
    params: ReadStream,
) -> io::Result<()>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
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

    let (read_stream, read_queue) = tokio::sync::mpsc::unbounded_channel();
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
        if read_stream.send(entry).is_err() {
            break;
        }
    }

    Ok(())
}

fn direct_read<WAL>(
    wal: WALRef<WAL>,
    send_result: oneshot::Sender<ReadStreamCompleted>,
    params: ReadStream,
) -> io::Result<()>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
{
    let (read_stream, read_queue) = tokio::sync::mpsc::unbounded_channel();
    let stream = MikoshiStream::new(read_queue);
    let _ = send_result.send(ReadStreamCompleted::Success(stream));

    let mut count = 0usize;
    let mut entries = wal.entries(params.starting.raw());

    while let Some(entry) = entries.next()? {
        let entry = match geth_domain::parse_event(&entry.payload) {
            Err(e) => {
                // TODO - Should crash the server entirely!
                tracing::error!("fatal error when parsing event from transaction log: {}", e);
                panic!("unrecoverable error when reading the transaction log");
            }

            Ok(data) => match data.event.unwrap() {
                Event::RecordedEvent(event) => Entry {
                    id: event.id.into(),
                    r#type: event.class,
                    stream_name: event.stream_name,
                    revision: event.revision,
                    data: event.data,
                    position: Position(entry.position),
                    created: Utc.timestamp_opt(event.created, 0).unwrap(),
                },

                Event::StreamDeleted(event) => Entry {
                    id: Uuid::nil(),
                    r#type: names::types::STREAM_DELETED.to_string(),
                    stream_name: event.stream_name,
                    revision: u64::MAX,
                    data: Default::default(),
                    position: Position(entry.position),
                    created: Utc.timestamp_opt(event.created, 0).unwrap(),
                },
            },
        };

        if read_stream.send(entry).is_err() {
            tracing::warn!(
                "read operation {} interrupted because nobody is listening to mikoshi stream",
                params.correlation
            );

            break;
        }

        count += 1;

        if count >= params.count {
            break;
        }
    }

    Ok(())
}
