use std::io;

use tokio::task::spawn_blocking;

use geth_common::{ExpectedRevision, Position, WriteResult, WrongExpectedRevisionError};
use geth_domain::DomainRef;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};

use crate::messages::{AppendStream, AppendStreamCompleted, DeleteStream, DeleteStreamCompleted};
use crate::process::storage::index::{CurrentRevision, StorageIndex};
use crate::process::storage::RevisionCache;

#[derive(Clone)]
pub struct StorageWriter<WAL, S>
where
    S: Storage,
{
    wal: WALRef<WAL>,
    index: StorageIndex<S>,
    revision_cache: RevisionCache,
    domain_ref: DomainRef,
}

impl<WAL, S> StorageWriter<WAL, S>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    pub fn new(wal: WALRef<WAL>, index: StorageIndex<S>, revision_cache: RevisionCache) -> Self {
        Self {
            wal,
            index,
            revision_cache,
            domain_ref: DomainRef::new(),
        }
    }

    pub async fn append(&self, params: AppendStream) -> AppendStreamCompleted {
        let wal = self.wal.clone();
        let index = self.index.clone();
        let revision_cache = self.revision_cache.clone();
        let domain_ref = self.domain_ref.clone();

        let handle = spawn_blocking(move || {
            let current_revision = index.stream_current_revision(&params.stream_name)?;

            if current_revision.is_deleted() {
                return Ok::<_, io::Error>(AppendStreamCompleted::StreamDeleted);
            }

            if let Some(e) = optimistic_concurrency_check(params.expected, current_revision) {
                return Ok(AppendStreamCompleted::Failure(e));
            }

            let mut revision = current_revision.next_revision();
            let mut events = Vec::with_capacity(params.events.len());
            let mut domain = domain_ref.lock().unwrap();
            let stream_name = domain.create_shared_string(&params.stream_name);

            for event in &params.events {
                let class = domain.create_string(event.r#type.as_str());
                let event = domain
                    .events()
                    .recorded_event(event.id, stream_name, class, revision);

                events.push(event);

                revision += 1;
            }

            let receipt = wal.append(events)?;
            index.chase(receipt.next_position);

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
            Ok(result) => result.unwrap_or_else(|e| {
                AppendStreamCompleted::Unexpected(eyre::eyre!("I/O error: {}", e))
            }),
        }
    }

    pub async fn delete(&self, params: DeleteStream) -> DeleteStreamCompleted {
        let index = self.index.clone();
        let revision_cache = self.revision_cache.clone();
        let wal = self.wal.clone();
        let domain_ref = self.domain_ref.clone();

        let handle = spawn_blocking(move || {
            let current_revision = index.stream_current_revision(&params.stream_name)?;
            let mut domain = domain_ref.lock().unwrap();

            if let Some(e) = optimistic_concurrency_check(params.expected, current_revision) {
                return Ok::<_, io::Error>(DeleteStreamCompleted::Failure(e));
            }

            let evt = domain
                .events()
                .stream_deleted(&params.stream_name, current_revision.next_revision());

            let receipt = wal.append(vec![evt])?;

            revision_cache.insert(params.stream_name, u64::MAX);
            index.chase(receipt.next_position);

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
            Ok(result) => result.unwrap_or_else(|e| {
                DeleteStreamCompleted::Unexpected(eyre::eyre!("I/O error: {}", e))
            }),
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
