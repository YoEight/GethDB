use std::{io, sync, thread};

use chrono::Utc;
use tokio::task::spawn_blocking;

use geth_common::{ExpectedRevision, Position, WriteResult, WrongExpectedRevisionError};
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};

use crate::messages::{AppendStream, AppendStreamCompleted, DeleteStream, DeleteStreamCompleted};
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
    builder: flatbuffers::FlatBufferBuilder<'static>,
}

impl<WAL, S> StorageWriterInternal<WAL, S>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    pub fn new(wal: WALRef<WAL>, index: StorageIndex<S>, revision_cache: RevisionCache) -> Self {
        Self {
            wal,
            index,
            revision_cache,
            builder: flatbuffers::FlatBufferBuilder::with_capacity(4_096),
        }
    }

    pub fn append(&mut self, params: AppendStream) -> io::Result<AppendStreamCompleted> {
        let current_revision = self.index.stream_current_revision(&params.stream_name)?;

        if current_revision.is_deleted() {
            return Ok::<_, io::Error>(AppendStreamCompleted::StreamDeleted);
        }

        if let Some(e) = optimistic_concurrency_check(params.expected, current_revision) {
            return Ok(AppendStreamCompleted::Failure(e));
        }

        let mut revision = current_revision.next_revision();
        let mut events = Vec::with_capacity(params.events.len());
        let stream_name = self.builder.create_shared_string(&params.stream_name);
        let created = Utc::now().timestamp();

        for event in &params.events {
            let class = self.builder.create_string(&event.r#type);
            let (most, least) = event.id.as_u64_pair();
            let mut id = geth_domain::binary::Id::default();

            id.set_high(most);
            id.set_low(least);

            let wire_event = geth_domain::binary::RecordedEvent::create(
                &mut self.builder,
                &mut geth_domain::binary::RecordedEventArgs {
                    id: Some(&id),
                    revision,
                    stream_name: Some(stream_name),
                    class: Some(class),
                    created,
                    data: Some(self.builder.create_vector(&event.data)),
                    metadata: None,
                },
            )
            .as_union_value();

            let serialized = geth_domain::binary::Event::create(
                &mut self.builder,
                &mut geth_domain::binary::EventArgs {
                    event_type: geth_domain::binary::Events::RecordedEvent,
                    event: Some(wire_event),
                },
            );

            self.builder.finish_minimal(serialized);
            events.push(self.builder.finished_data());

            revision += 1;
        }

        let receipt = self.wal.append(events)?;
        self.index.chase(receipt.next_position);

        self.revision_cache.insert(params.stream_name, revision - 1);

        Ok(AppendStreamCompleted::Success(WriteResult {
            next_expected_version: ExpectedRevision::Revision(revision),
            position: Position(receipt.start_position),
            next_logical_position: receipt.next_position,
        }))
    }

    pub fn delete(&mut self, params: DeleteStream) -> io::Result<DeleteStreamCompleted> {
        let current_revision = self.index.stream_current_revision(&params.stream_name)?;

        if let Some(e) = optimistic_concurrency_check(params.expected, current_revision) {
            return Ok::<_, io::Error>(DeleteStreamCompleted::Failure(e));
        }

        let created = Utc::now().timestamp();
        let stream_name = self.builder.create_string(&params.stream_name);
        let wire_event = geth_domain::binary::StreamDeleted::create(
            &mut self.builder,
            &mut geth_domain::binary::StreamDeletedArgs {
                stream_name: Some(stream_name),
                revision: current_revision.next_revision(),
                created,
            },
        )
        .as_union_value();

        let serialized = geth_domain::binary::Event::create(
            &mut self.builder,
            &mut geth_domain::binary::EventArgs {
                event_type: geth_domain::binary::Events::StreamDeleted,
                event: Some(wire_event),
            },
        );

        self.builder.finish_minimal(serialized);
        let receipt = self.wal.append(vec![self.builder.finished_data()])?;

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
    let (sender, mut mailbox) = sync::mpsc::channel();
    let mut internal = StorageWriterInternal {
        wal,
        index,
        revision_cache,
        builder: flatbuffers::FlatBufferBuilder::with_capacity(4_096),
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
                Ok(AppendStreamCompleted::Unexpected(eyre::eyre!(
                    "storage writer process exited"
                )))
            }
        } else {
            Ok(AppendStreamCompleted::Unexpected(eyre::eyre!(
                "storage writer process exited"
            )))
        }
    }

    pub fn delete(&self, params: DeleteStream) -> io::Result<DeleteStreamCompleted> {
        let (sender, resp) = sync::mpsc::sync_channel(1);

        if self.inner.send(Msg::Delete(params, sender)).is_ok() {
            if let Ok(result) = resp.recv() {
                result
            } else {
                Ok(DeleteStreamCompleted::Unexpected(eyre::eyre!(
                    "storage writer process exited"
                )))
            }
        } else {
            Ok(DeleteStreamCompleted::Unexpected(eyre::eyre!(
                "storage writer process exited"
            )))
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
