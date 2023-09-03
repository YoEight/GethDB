use crate::messages::{AppendStream, AppendStreamCompleted, DeleteStream, DeleteStreamCompleted};
use crate::process::storage::service::current::CurrentRevision;
use crate::process::storage::RevisionCache;
use chrono::Utc;
use geth_common::{ExpectedRevision, Position, WriteResult, WrongExpectedRevisionError};
use geth_mikoshi::domain::{StreamDeleted, StreamEventAppended};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::index::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};
use std::io;
use tokio::task::spawn_blocking;
use uuid::Uuid;

pub struct StorageWriter<WAL, S> {
    wal: WALRef<WAL>,
    index: Lsm<S>,
    revision_cache: RevisionCache,
}

impl<WAL, S> StorageWriter<WAL, S>
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

    pub async fn append(&self, params: AppendStream) -> AppendStreamCompleted {
        let wal = self.wal.clone();
        let index = self.index.clone();
        let revision_cache = self.revision_cache.clone();

        let handle = spawn_blocking(move || {
            let stream_key = mikoshi_hash(&params.stream_name);
            let current_revision = if let Some(current) = revision_cache.get(&params.stream_name) {
                CurrentRevision::Revision(current)
            } else {
                index
                    .highest_revision(stream_key)?
                    .map_or_else(|| CurrentRevision::NoStream, CurrentRevision::Revision)
            };

            if current_revision.is_deleted() {
                return Ok::<_, io::Error>(AppendStreamCompleted::StreamDeleted);
            }

            if let Some(e) = optimistic_concurrency_check(params.expected, current_revision) {
                return Ok(AppendStreamCompleted::Failure(e));
            }

            let created = Utc::now().timestamp();
            let mut revision = current_revision.next_revision();
            let transaction_id = Uuid::new_v4();
            let mut transaction_offset = 0u16;
            let mut events = Vec::with_capacity(params.events.len());

            for event in params.events {
                events.push(StreamEventAppended {
                    revision,
                    event_stream_id: params.stream_name.clone(),
                    transaction_id,
                    transaction_offset,
                    event_id: event.id,
                    created,
                    event_type: event.r#type,
                    data: event.data,
                    metadata: Default::default(),
                });

                revision += 1;
                transaction_offset += 1;
            }

            let receipt = wal.append(events.as_slice())?;
            // TODO - Move to implemented indexing the previous way.
            // let index_entries =
            //     receipt
            //         .mappings
            //         .into_iter()
            //         .enumerate()
            //         .map(|(offset, position)| {
            //             (stream_key, starting_revision + offset as u64, position)
            //         });

            revision_cache.insert(params.stream_name, revision - 1);

            Ok(AppendStreamCompleted::Success(WriteResult {
                next_expected_version: ExpectedRevision::Revision(revision),
                position: Position(receipt.start_position),
                next_logical_position: receipt.next_position,
            }))
        });

        match handle.await {
            Err(_) => {
                AppendStreamCompleted::Unexpected(eyre::eyre!("I/O write operation became unbound"))
            }
            Ok(result) => match result {
                Err(e) => AppendStreamCompleted::Unexpected(eyre::eyre!("I/O error: {}", e)),
                Ok(v) => v,
            },
        }
    }

    pub async fn delete(&self, params: DeleteStream) -> DeleteStreamCompleted {
        let index = self.index.clone();
        let revision_cache = self.revision_cache.clone();
        let wal = self.wal.clone();

        let handle = spawn_blocking(move || {
            let stream_key = mikoshi_hash(&params.stream_name);
            let current_revision = index
                .highest_revision(stream_key)?
                .map_or_else(|| CurrentRevision::NoStream, CurrentRevision::Revision);

            if let Some(e) = optimistic_concurrency_check(params.expected, current_revision) {
                return Ok::<_, io::Error>(DeleteStreamCompleted::Failure(e));
            }

            let receipt = wal.append(&[StreamDeleted {
                revision: current_revision.next_revision(),
                event_stream_id: params.stream_name.clone(),
                created: Utc::now().timestamp(),
            }])?;

            revision_cache.insert(params.stream_name, u64::MAX);

            Ok(DeleteStreamCompleted::Success(WriteResult {
                next_expected_version: ExpectedRevision::Revision(current_revision.next_revision()),
                position: Position(receipt.start_position),
                next_logical_position: receipt.next_position,
            }))
        });

        match handle.await {
            Err(_) => {
                DeleteStreamCompleted::Unexpected(eyre::eyre!("I/O write operation became unbound"))
            }
            Ok(result) => match result {
                Err(e) => DeleteStreamCompleted::Unexpected(eyre::eyre!("I/O error: {}", e)),
                Ok(v) => v,
            },
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
