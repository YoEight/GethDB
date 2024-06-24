use std::{io, sync, thread};

use bytes::BytesMut;
use chrono::Utc;
use prost::Message;

use geth_common::{
    AppendError, AppendStreamCompleted, DeleteError, DeleteStreamCompleted, ExpectedRevision,
    Position, WriteResult, WrongExpectedRevisionError,
};
use geth_domain::AppendProposes;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};

use crate::messages::{AppendStream, DeleteStream};
use crate::process::storage::index::{CurrentRevision, StorageIndex};
use crate::process::storage::RevisionCache;

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
    index: StorageIndex<S>,
    revision_cache: RevisionCache,
    buffer: BytesMut,
}

impl<WAL, S> StorageWriterInternal<WAL, S>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    pub fn append(&mut self, params: AppendStream) -> io::Result<AppendStreamCompleted> {
        let current_revision = self.index.stream_current_revision(&params.stream_name)?;

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

        let receipt = self.wal.append(events)?;
        self.index.chase(receipt.next_position);

        if len > 0 {
            self.revision_cache
                .insert(params.stream_name, revision + len - 1);
        }

        Ok(AppendStreamCompleted::Success(WriteResult {
            next_expected_version: ExpectedRevision::Revision(revision + len),
            position: Position(receipt.start_position),
            next_logical_position: receipt.next_position,
        }))
    }

    pub fn delete(&mut self, params: DeleteStream) -> io::Result<DeleteStreamCompleted> {
        let current_revision = self.index.stream_current_revision(&params.stream_name)?;

        if let Some(e) = optimistic_concurrency_check(params.expected, current_revision) {
            return Ok::<_, io::Error>(DeleteStreamCompleted::Error(
                DeleteError::WrongExpectedRevision(e),
            ));
        }

        let created = Utc::now().timestamp();
        let event = geth_domain::binary::events::StreamDeleted {
            stream_name: params.stream_name.clone(),
            revision: current_revision.next_revision(),
            created,
        };

        let event = geth_domain::binary::events::Events {
            event: Some(geth_domain::binary::events::Event::StreamDeleted(event)),
        };

        event.encode(&mut self.buffer).unwrap();
        let receipt = self.wal.append(vec![self.buffer.split().freeze()])?;

        self.revision_cache.insert(params.stream_name, u64::MAX);
        self.index.chase(receipt.next_position);

        Ok(DeleteStreamCompleted::Success(WriteResult {
            next_expected_version: ExpectedRevision::Revision(current_revision.next_revision()),
            position: Position(receipt.start_position),
            next_logical_position: receipt.next_position,
        }))
    }
}

pub fn new_storage_writer<WAL, S>(
    wal: WALRef<WAL>,
    index: StorageIndex<S>,
    revision_cache: RevisionCache,
) -> StorageWriter
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let (sender, mailbox) = sync::mpsc::channel();
    let mut internal = StorageWriterInternal {
        wal,
        index,
        revision_cache,
        buffer: BytesMut::with_capacity(4_096),
    };

    thread::spawn(move || {
        while let Some(msg) = mailbox.recv().ok() {
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
