use std::io;
use std::sync::mpsc;
use uuid::Uuid;

use geth_common::{
    Direction, ExpectedRevision, Position, Revision, WriteResult, WrongExpectedRevisionError,
};
use geth_mikoshi::{
    hashing::mikoshi_hash,
    index::{IteratorIO, Lsm},
    storage::Storage,
    wal::ChunkManager,
    Entry, MikoshiStream,
};

use crate::bus::ReadStreamMsg;
use crate::messages::{ReadStream, ReadStreamCompleted};
use crate::{
    bus::AppendStreamMsg,
    messages::{AppendStream, AppendStreamCompleted},
};

pub struct StorageReaderService<S> {
    manager: ChunkManager<S>,
    index: Lsm<S>,
}

impl<S> StorageReaderService<S>
where
    S: Storage + Send + Sync + 'static,
{
    pub fn new(manager: ChunkManager<S>, index: Lsm<S>) -> Self {
        Self { manager, index }
    }

    pub fn read(&mut self, params: ReadStream) -> io::Result<ReadStreamCompleted> {
        match params.direction {
            Direction::Backward => todo!(),
            Direction::Forward => {
                self.read_forward(params.stream_name, params.starting, params.count)
            }
        }
    }

    pub fn read_forward(
        &mut self,
        stream_name: String,
        start: Revision<u64>,
        count: usize,
    ) -> io::Result<ReadStreamCompleted> {
        let key = mikoshi_hash(&stream_name);
        let starting_from = match start {
            Revision::End => unreachable!(),
            Revision::Start => 0,
            Revision::Revision(rev) => rev,
        };

        let mut iter = self
            .index
            .scan(key, starting_from..starting_from + count as u64);

        let manager = self.manager.clone();
        let (read_stream, read_queue) = tokio::sync::mpsc::channel(500);
        let stream = MikoshiStream::new(read_queue);

        tokio::task::spawn_blocking(move || {
            while let Some(entry) = iter.next()? {
                let record = manager.read_at(entry.position)?;
                let entry = Entry {
                    id: record.event_id,
                    r#type: record.event_type,
                    stream_name: record.event_stream_id,
                    revision: record.revision,
                    data: record.data,
                    position: Position(entry.position),
                    created: record.created,
                };

                // if failing means that we don't need to read form the transaction log.
                if read_stream.blocking_send(entry).is_err() {
                    break;
                }
            }

            Ok::<_, io::Error>(())
        });

        Ok(ReadStreamCompleted {
            correlation: Uuid::new_v4(), // TODO - keep using the same correlation id we received in the request.
            reader: stream,
        })
    }
}

pub fn start<S>(manager: ChunkManager<S>, index: Lsm<S>) -> mpsc::Sender<ReadStreamMsg>
where
    S: Storage + Send + Sync + 'static,
{
    let (sender, recv) = mpsc::channel();
    let service = StorageReaderService::new(manager, index);

    tokio::task::spawn_blocking(|| process(service, recv));

    sender
}

fn process<S>(
    mut service: StorageReaderService<S>,
    queue: mpsc::Receiver<ReadStreamMsg>,
) -> io::Result<()>
where
    S: Storage + Send + Sync + 'static,
{
    while let Ok(msg) = queue.recv() {
        let result = service.read(msg.payload)?;
        let _ = msg.mail.send(result);
    }

    Ok(())
}
