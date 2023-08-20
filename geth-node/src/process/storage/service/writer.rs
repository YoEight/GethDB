use chrono::Utc;
use std::io;
use std::sync::mpsc;
use uuid::Uuid;

use geth_common::{ExpectedRevision, Position, WriteResult, WrongExpectedRevisionError};
use geth_mikoshi::domain::StreamEventAppended;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};
use geth_mikoshi::{hashing::mikoshi_hash, index::Lsm, storage::Storage};

use crate::bus::DeleteStreamMsg;
use crate::messages::{DeleteStream, DeleteStreamCompleted};
use crate::{
    bus::AppendStreamMsg,
    messages::{AppendStream, AppendStreamCompleted},
};

pub enum WriteRequests {
    WriteStream(AppendStreamMsg),
    DeleteStream(DeleteStreamMsg),
}

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
pub struct StorageWriterService<WAL, S> {
    wal: WALRef<WAL>,
    index: Lsm<S>,
}

impl<WAL, S> StorageWriterService<WAL, S>
where
    WAL: WriteAheadLog,
    S: Storage + Send + Sync + 'static,
{
    pub fn new(wal: WALRef<WAL>, index: Lsm<S>) -> Self {
        Self { wal, index }
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

        let receipt = self.wal.append(events.as_slice())?;

        Ok(AppendStreamCompleted::Success(WriteResult {
            next_expected_version: ExpectedRevision::Revision(revision),
            position: Position(receipt.position),
            next_logical_position: receipt.next_position,
        }))
    }

    pub fn delete(&mut self, _params: DeleteStream) -> io::Result<DeleteStreamCompleted> {
        todo!()
    }
}

pub fn start<WAL, S>(
    wal: WALRef<WAL>,
    index: Lsm<S>,
    index_queue: mpsc::Sender<u64>,
) -> mpsc::Sender<WriteRequests>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let (sender, recv) = mpsc::channel();
    let service = StorageWriterService::new(wal, index);

    tokio::task::spawn_blocking(|| process(service, index_queue, recv));

    sender
}

fn process<WAL, S>(
    mut service: StorageWriterService<WAL, S>,
    index_queue: mpsc::Sender<u64>,
    queue: mpsc::Receiver<WriteRequests>,
) -> io::Result<()>
where
    WAL: WriteAheadLog + Send + Sync,
    S: Storage + Send + Sync + 'static,
{
    while let Ok(msg) = queue.recv() {
        match msg {
            WriteRequests::WriteStream(msg) => {
                let stream_name = msg.payload.stream_name.clone();
                match service.append(msg.payload) {
                    Err(e) => {
                        let _ = msg.mail.send(Err(eyre::eyre!(
                            "Error when appending to '{}': {}",
                            stream_name,
                            e
                        )));
                    }
                    Ok(result) => {
                        if let AppendStreamCompleted::Success(result) = &result {
                            let _ = index_queue.send(result.next_logical_position);
                        }

                        let _ = msg.mail.send(Ok(result));
                    }
                }
            }

            WriteRequests::DeleteStream(msg) => {
                let stream_name = msg.payload.stream_name.clone();
                match service.delete(msg.payload) {
                    Err(e) => {
                        let _ = msg.mail.send(Err(eyre::eyre!(
                            "Error when appending to '{}': {}",
                            stream_name,
                            e
                        )));
                    }
                    Ok(result) => {
                        if let DeleteStreamCompleted::Success(_result) = &result {
                            // TODO - report to indexing.
                        }

                        let _ = msg.mail.send(Ok(result));
                    }
                }
            }
        }
    }

    Ok(())
}
