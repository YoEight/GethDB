use std::io;

use geth_domain::index::BlockEntry;
use tokio::sync::oneshot;
use tokio::task::spawn_blocking;
use uuid::Uuid;

use geth_common::{Direction, IteratorIO, Position, Record};
use geth_domain::binary::models::Event;
use geth_domain::RecordedEvent;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};
use geth_mikoshi::{storage::Storage, MikoshiStream};

use crate::domain::index::IndexRef;
use crate::messages::{ReadStream, ReadStreamCompleted};
use crate::names;

#[derive(Clone)]
pub struct StorageReader<WAL, S>
where
    S: Storage,
{
    wal: WALRef<WAL>,
    index: IndexRef<S>,
}

impl<WAL, S> StorageReader<WAL, S>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    pub fn new(wal: WALRef<WAL>, index: IndexRef<S>) -> Self {
        Self { wal, index }
    }

    pub async fn read(&self, params: ReadStream) -> ReadStreamCompleted {
        let index = self.index.clone();
        let wal = self.wal.clone();
        let (send_result, recv_result) = tokio::sync::oneshot::channel();

        spawn_blocking(move || {
            if params.stream_name == names::streams::ALL
                || params.stream_name == names::streams::SYSTEM
            {
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
    index_ref: IndexRef<S>,
    wal: WALRef<WAL>,
    send_result: oneshot::Sender<ReadStreamCompleted>,
    params: ReadStream,
) -> io::Result<()>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let index = index_ref.inner.read().unwrap();
    let current_revision = index.stream_current_revision(&params.stream_name)?;

    if current_revision.is_deleted() {
        let _ = send_result.send(ReadStreamCompleted::StreamDeleted);
        return Ok::<_, io::Error>(());
    }

    let mut iter: Box<dyn IteratorIO<Item = BlockEntry>> = match params.direction {
        Direction::Forward => {
            Box::new(index.scan_forward(&params.stream_name, params.starting.raw(), params.count))
        }
        Direction::Backward => {
            Box::new(index.scan_backward(&params.stream_name, params.starting.raw(), params.count))
        }
    };

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

        let record = Record {
            id: event.id,
            r#type: event.class,
            stream_name: event.stream_name,
            revision: event.revision,
            data: event.data,
            position: Position(record.position),
        };

        // if failing means that we don't need to read form the transaction log.
        if read_stream.send(record).is_err() {
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
                Event::RecordedEvent(event) => Record {
                    id: event.id.into(),
                    r#type: event.class,
                    stream_name: event.stream_name,
                    revision: event.revision,
                    data: event.data,
                    position: Position(entry.position),
                },

                Event::StreamDeleted(event) => Record {
                    id: Uuid::nil(),
                    r#type: names::types::STREAM_DELETED.to_string(),
                    stream_name: event.stream_name,
                    revision: u64::MAX,
                    data: Default::default(),
                    position: Position(entry.position),
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
