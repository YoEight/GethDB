use crate::process::indexing::{IndexClient, Indexing};
use crate::process::start_process_manager;
use crate::process::writing::Writing;
use bytes::BytesMut;
use geth_common::Direction;
use geth_mikoshi::wal::chunks::ChunkBasedWAL;
use geth_mikoshi::InMemoryStorage;

#[tokio::test]
async fn test_write_read() -> eyre::Result<()> {
    let mut buffer = BytesMut::new();
    let manager = start_process_manager();
    let storage = InMemoryStorage::new();
    let proc_id = manager.spawn_raw(Indexing::new(storage.clone())).await?;
    let mut client = IndexClient::new(proc_id, manager.clone(), buffer.split());
    let wal = ChunkBasedWAL::load(storage.clone());
    let writer_id = manager.spawn_raw(Writing::new(proc_id)).await?;
    let mut expected = vec![];

    for i in 0..10 {
        expected.push((i, i + 10));
    }

    client.store(2, expected.clone()).await?;
    let entries = client
        .read(2, 0, usize::MAX, Direction::Forward)
        .await?
        .collect()
        .await?;

    assert_eq!(expected, entries);

    Ok(())
}
