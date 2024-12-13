use std::sync::{atomic::AtomicU64, Arc};

use bytes::BytesMut;
use geth_mikoshi::{
    wal::{chunks::ChunkBasedWAL, WALRef},
    InMemoryStorage,
};

use crate::process::{
    indexing::{IndexClient, Indexing},
    start_process_manager,
};

#[tokio::test]
async fn test_simple_indexing() -> eyre::Result<()> {
    let mut buffer = BytesMut::new();
    let manager = start_process_manager();
    let storage = InMemoryStorage::new();
    let wal = ChunkBasedWAL::load(storage.clone())?;
    let wal = WALRef::new(wal);
    let writer_chk = Arc::new(AtomicU64::new(0));

    let proc_id = manager
        .spawn_raw(Indexing::new(storage, wal.clone(), writer_chk))
        .await?;

    let client = IndexClient::new(proc_id, manager.clone(), buffer.split());

    Ok(())
}
