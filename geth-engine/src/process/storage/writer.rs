use std::{io, sync, thread};

use bytes::BytesMut;
use chrono::Utc;
use prost::Message;
use uuid::Uuid;

use geth_common::{
    AppendError, AppendStreamCompleted, DeleteError, DeleteStreamCompleted, ExpectedRevision,
    Position, Record, WriteResult, WrongExpectedRevisionError,
};
use geth_domain::AppendProposes;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{LogReceipt, WALRef, WriteAheadLog};

use crate::domain::index::{CurrentRevision, IndexRef};
use crate::messages::{AppendStream, DeleteStream};
use crate::names;
use crate::process::SubscriptionsClient;

enum Msg {
    Append(
        AppendStream,
        sync::mpsc::SyncSender<io::Result<AppendStreamCompleted>>,
    ),

    Delete(
        DeleteStream,
        sync::mpsc::SyncSender<io::Result<DeleteStreamCompleted>>,
    ),
}

pub struct StorageWriterInternal<WAL, S>
where
    S: Storage,
{
    wal: WALRef<WAL>,
    index: IndexRef<S>,
    buffer: BytesMut,
    sub_client: SubscriptionsClient,
}

impl<WAL, S> StorageWriterInternal<WAL, S>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    pub fn append(&mut self, params: AppendStream) -> io::Result<AppendStreamCompleted> {
        let current_revision = self
            .index
            .inner
            .read()
            .unwrap()
            .stream_current_revision(&params.stream_name)?;

        if current_revision.is_deleted() {
            return Ok::<_, io::Error>(AppendStreamCompleted::Error(AppendError::StreamDeleted));
        }

        if let Some(e) = optimistic_concurrency_check(params.expected, current_revision) {
            return Ok(AppendStreamCompleted::Error(
                AppendError::WrongExpectedRevision(e),
            ));
        }

        let revision = current_revision.next_revision();
        let len = params.events.len() as u64;

        let events = AppendProposes::new(
            params.stream_name.clone(),
            Utc::now(),
            revision,
            &mut self.buffer,
            params.events.into_iter(),
        );

        let receipt: LogReceipt = todo!();
        // let receipt = self.wal.append(events)?;
        let _ = self.sub_client.event_written(Record {
            id: Uuid::new_v4(),
            r#type: names::types::EVENTS_WRITTEN.to_string(),
            stream_name: names::streams::SYSTEM.to_string(),
            position: Position(receipt.start_position),
            revision: receipt.start_position,
            data: Default::default(),
        });

        if len > 0 {
            self.index
                .inner
                .read()
                .unwrap()
                .cache_stream_revision(params.stream_name, revision + len - 1);
        }

        Ok(AppendStreamCompleted::Success(WriteResult {
            next_expected_version: ExpectedRevision::Revision(revision + len),
            position: Position(receipt.start_position),
            next_logical_position: receipt.next_position,
        }))
    }

    pub fn delete(&mut self, params: DeleteStream) -> io::Result<DeleteStreamCompleted> {
        let current_revision = self
            .index
            .inner
            .read()
            .unwrap()
            .stream_current_revision(&params.stream_name)?;

        if let Some(e) = optimistic_concurrency_check(params.expected, current_revision) {
            return Ok::<_, io::Error>(DeleteStreamCompleted::Error(
                DeleteError::WrongExpectedRevision(e),
            ));
        }

        let created = Utc::now().timestamp();
        let event = geth_domain::binary::models::StreamDeleted {
            stream_name: params.stream_name.clone(),
            revision: current_revision.next_revision(),
            created,
        };

        let event = geth_domain::binary::models::Events {
            event: Some(geth_domain::binary::models::Event::StreamDeleted(event)),
        };

        event.encode(&mut self.buffer).unwrap();
        let receipt: LogReceipt = todo!(); // = self.wal.append(vec![self.buffer.split().freeze()])?;

        self.index
            .inner
            .read()
            .unwrap()
            .cache_stream_revision(params.stream_name.clone(), u64::MAX);
        let _ = self.sub_client.event_written(Record {
            id: Uuid::new_v4(),
            r#type: names::types::STREAM_DELETED.to_string(),
            stream_name: params.stream_name,
            position: Position(receipt.start_position),
            revision: receipt.start_position,
            data: Default::default(),
        });

        Ok(DeleteStreamCompleted::Success(WriteResult {
            next_expected_version: ExpectedRevision::Revision(current_revision.next_revision()),
            position: Position(receipt.start_position),
            next_logical_position: receipt.next_position,
        }))
    }
}

pub fn new_storage_writer<WAL, S>(
    wal: WALRef<WAL>,
    index: IndexRef<S>,
    sub_client: SubscriptionsClient,
) -> StorageWriter
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let (sender, mailbox) = sync::mpsc::channel();
    let mut internal = StorageWriterInternal {
        wal,
        index,
        sub_client,
        buffer: BytesMut::with_capacity(4_096),
    };

    thread::spawn(move || {
        while let Ok(msg) = mailbox.recv() {
            match msg {
                Msg::Append(params, resp) => {
                    let result = internal.append(params);
                    let _ = resp.send(result);
                }

                Msg::Delete(params, resp) => {
                    let result = internal.delete(params);
                    let _ = resp.send(result);
                }
            }
        }

        tracing::info!("storage writer process exited");
    });

    StorageWriter { inner: sender }
}

#[derive(Clone)]
pub struct StorageWriter {
    inner: sync::mpsc::Sender<Msg>,
}

impl StorageWriter {
    pub fn append(&self, params: AppendStream) -> io::Result<AppendStreamCompleted> {
        let (sender, resp) = sync::mpsc::sync_channel(1);

        if self.inner.send(Msg::Append(params, sender)).is_ok() {
            if let Ok(result) = resp.recv() {
                result
            } else {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "storage writer process exited",
                ))
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "storage writer process exited",
            ))
        }
    }

    pub fn delete(&self, params: DeleteStream) -> io::Result<DeleteStreamCompleted> {
        let (sender, resp) = sync::mpsc::sync_channel(1);

        if self.inner.send(Msg::Delete(params, sender)).is_ok() {
            if let Ok(result) = resp.recv() {
                result
            } else {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "storage writer process exited",
                ))
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "storage writer process exited",
            ))
        }
    }
}

fn optimistic_concurrency_check(
    expected: ExpectedRevision,
    current: CurrentRevision,
) -> Option<WrongExpectedRevisionError> {
    match (expected, current) {
        (ExpectedRevision::NoStream, CurrentRevision::NoStream) => None,
        (ExpectedRevision::StreamExists, CurrentRevision::Revision(_)) => None,
        (ExpectedRevision::Any, _) => None,
        (ExpectedRevision::Revision(a), CurrentRevision::Revision(b)) if a == b => None,
        _ => Some(WrongExpectedRevisionError {
            expected,
            current: current.as_expected(),
        }),
    }
}
