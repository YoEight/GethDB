use crate::process::storage::InMemoryCheckpoint;
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::index::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::ChunkManager;
use std::io;
use std::sync::atomic::AtomicU64;
use std::sync::{mpsc, Arc};

pub struct StorageIndexService<S> {
    manager: ChunkManager<S>,
    index: Lsm<S>,
}

impl<S> StorageIndexService<S>
where
    S: Storage + Send + Sync + 'static,
{
    pub fn new(manager: ChunkManager<S>, index: Lsm<S>) -> Self {
        Self { manager, index }
    }

    pub fn chase(&mut self, target: u64) -> io::Result<()> {
        let mut records = self.manager.prepare_logs(target);

        while let Some((position, record)) = records.next()? {
            let key = mikoshi_hash(&record.event_stream_id);
            self.index.put(key, record.revision, position)?;
        }

        Ok(())
    }
}

pub fn start<S>(manager: ChunkManager<S>, index: Lsm<S>) -> mpsc::Sender<u64>
where
    S: Storage + Send + Sync + 'static,
{
    let (sender, recv) = mpsc::channel();
    let checkpoint = index.checkpoint();
    let service = StorageIndexService::new(manager, index);

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

        checkpoint = msg;
        service.chase(msg)?;
    }

    Ok(())
}
