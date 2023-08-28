use crate::messages::{AppendStream, AppendStreamCompleted};
use crate::process::storage::service::current::CurrentRevision;
use crate::process::storage::RevisionCache;
use chrono::Utc;
use geth_common::{ExpectedRevision, Position, WriteResult, WrongExpectedRevisionError};
use geth_mikoshi::domain::StreamEventAppended;
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::index::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};
use std::io;
use uuid::Uuid;

pub struct StorageWriter<WAL, S> {
    wal: WALRef<WAL>,
    index: Lsm<S>,
    revision_cache: RevisionCache,
}

impl<WAL, S> StorageWriter<WAL, S>
where
    WAL: WriteAheadLog,
    S: Storage + Send + Sync + 'static,
{
    pub fn append(&mut self, params: AppendStream) -> io::Result<AppendStreamCompleted> {
        let stream_key = mikoshi_hash(&params.stream_name);
        let current_revision = if let Some(current) = self.revision_cache.get(&params.stream_name) {
            CurrentRevision::Revision(current)
        } else {
            self.index
                .highest_revision(stream_key)?
                .map_or_else(|| CurrentRevision::NoStream, CurrentRevision::Revision)
        };

        if current_revision.is_deleted() {
            return Ok(AppendStreamCompleted::StreamDeleted);
        }

        if let Some(e) = optimistic_concurrency_check(params.expected, current_revision) {
            return Ok(AppendStreamCompleted::Failure(e));
        }

        let created = Utc::now().timestamp();
        let mut revision = current_revision.next_revision();
        let starting_revision = revision;
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

        let receipt = self.wal.append(events.as_slice())?;
        let index_entries = receipt
            .mappings
            .into_iter()
            .enumerate()
            .map(|(offset, position)| (stream_key, starting_revision + offset as u64, position));

        self.revision_cache.insert(params.stream_name, revision - 1);
        self.index.put_values(index_entries)?;

        Ok(AppendStreamCompleted::Success(WriteResult {
            next_expected_version: ExpectedRevision::Revision(revision),
            position: Position(receipt.start_position),
            next_logical_position: receipt.next_position,
        }))
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
