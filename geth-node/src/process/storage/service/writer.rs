use std::io;
use std::sync::mpsc;

use geth_common::{ExpectedRevision, Position, WriteResult, WrongExpectedRevisionError};
use geth_mikoshi::{hashing::mikoshi_hash, index::Lsm, storage::Storage, wal::ChunkManager};

use crate::{
    bus::AppendStreamMsg,
    messages::{AppendStream, AppendStreamCompleted},
};

#[derive(Copy, Clone)]
enum CurrentRevision {
    NoStream,
    Revision(u64),
}

impl CurrentRevision {
    fn next_revision(self) -> u64 {
        match self {
            CurrentRevision::NoStream => 0,
            CurrentRevision::Revision(r) => r + 1,
        }
    }

    fn as_expected(self) -> ExpectedRevision {
        match self {
            CurrentRevision::NoStream => ExpectedRevision::NoStream,
            CurrentRevision::Revision(v) => ExpectedRevision::Revision(v),
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
pub struct StorageWriterService<S> {
    manager: ChunkManager<S>,
    index: Lsm<S>,
}

impl<S> StorageWriterService<S>
where
    S: Storage + Send + Sync + 'static,
{
    pub fn new(manager: ChunkManager<S>, index: Lsm<S>) -> Self {
        Self { manager, index }
    }

    pub fn append(&mut self, params: AppendStream) -> io::Result<AppendStreamCompleted> {
        let stream_key = mikoshi_hash(&params.stream_name);
        let current_revision = self
            .index
            .highest_revision(stream_key)?
            .map_or_else(|| CurrentRevision::NoStream, CurrentRevision::Revision);

        if let Some(e) = optimistic_concurrency_check(params.expected, current_revision) {
            return Ok(AppendStreamCompleted::Failure(e));
        }

        let starting_revision = current_revision.next_revision();

        let result = self
            .manager
            .append(params.stream_name, starting_revision, params.events)?;

        Ok(AppendStreamCompleted::Success(result))
    }
}

pub fn start<S>(
    manager: ChunkManager<S>,
    index: Lsm<S>,
    index_queue: mpsc::Sender<u64>,
) -> mpsc::Sender<AppendStreamMsg>
where
    S: Storage + Send + Sync + 'static,
{
    let (sender, recv) = mpsc::channel();
    let service = StorageWriterService::new(manager, index);

    tokio::task::spawn_blocking(|| process(service, index_queue, recv));

    sender
}

fn process<S>(
    mut service: StorageWriterService<S>,
    index_queue: mpsc::Sender<u64>,
    queue: mpsc::Receiver<AppendStreamMsg>,
) -> io::Result<()>
where
    S: Storage + Send + Sync + 'static,
{
    while let Ok(msg) = queue.recv() {
        let result = service.append(msg.payload)?;

        if let AppendStreamCompleted::Success(result) = &result {
            let _ = index_queue.send(result.next_logical_position);
        }

        let _ = msg.mail.send(result);
    }

    Ok(())
}
