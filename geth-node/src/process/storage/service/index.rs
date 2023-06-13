use crate::process::subscriptions::SubscriptionsClient;
use chrono::{TimeZone, Utc};
use geth_common::Position;
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::index::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::ChunkManager;
use geth_mikoshi::{Entry, IteratorIO};
use std::io;
use std::sync::mpsc;

pub struct StorageIndexService<S> {
    manager: ChunkManager<S>,
    index: Lsm<S>,
    sub_client: SubscriptionsClient,
}

impl<S> StorageIndexService<S>
where
    S: Storage + Send + Sync + 'static,
{
    pub fn new(manager: ChunkManager<S>, index: Lsm<S>, sub_client: SubscriptionsClient) -> Self {
        Self {
            manager,
            index,
            sub_client,
        }
    }

    pub fn chase(&mut self, starting_position: u64) -> io::Result<()> {
        let records = self.manager.prepare_logs(starting_position).map(|record| {
            let key = mikoshi_hash(&record.event_stream_id);

            let _ = self.sub_client.event_committed(Entry {
                id: record.event_id,
                r#type: record.event_type,
                stream_name: record.event_stream_id,
                revision: record.revision,
                data: record.data,
                position: Position(record.logical_position),
                created: Utc.timestamp_opt(record.created, 0).unwrap(),
            });

            (key, record.revision, record.logical_position)
        });

        self.index.put(records)?;

        Ok(())
    }
}

pub fn start<S>(
    manager: ChunkManager<S>,
    index: Lsm<S>,
    sub_client: SubscriptionsClient,
) -> mpsc::Sender<u64>
where
    S: Storage + Send + Sync + 'static,
{
    let (sender, recv) = mpsc::channel();
    let checkpoint = index.checkpoint();
    let service = StorageIndexService::new(manager, index, sub_client);

    tokio::task::spawn_blocking(move || process(service, checkpoint, recv));

    sender
}

fn process<S>(
    mut service: StorageIndexService<S>,
    mut checkpoint: u64,
    queue: mpsc::Receiver<u64>,
) -> io::Result<()>
where
    S: Storage + Send + Sync + 'static,
{
    while let Ok(msg) = queue.recv() {
        if msg <= checkpoint {
            continue;
        }

        service.chase(checkpoint)?;
        checkpoint = msg;
    }

    Ok(())
}
