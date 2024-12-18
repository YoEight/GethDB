use crate::process::indexing::{IndexClient, Indexing};
use crate::process::start_process_manager;
use crate::process::writing::{WriterClient, Writing};
use bytes::{Bytes, BytesMut};
use geth_common::{AppendStreamCompleted, Direction, ExpectedRevision};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::wal::chunks::ChunkBasedWAL;
use geth_mikoshi::wal::WriteAheadLog;
use geth_mikoshi::InMemoryStorage;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Foo {
    baz: u32,
}

#[tokio::test]
async fn test_write_read() -> eyre::Result<()> {
    let _ = pretty_env_logger::init();
    let mut buffer = BytesMut::new();
    let manager = start_process_manager();
    let storage = InMemoryStorage::new();
    let proc_id = manager.spawn_raw(Indexing::new(storage.clone())).await?;
    let mut index_client = IndexClient::new(proc_id, manager.clone(), buffer.split());
    let wal = ChunkBasedWAL::load(storage.clone())?;
    let writer_id = manager.spawn_raw(Writing::new(wal)).await?;
    let mut writer_client = WriterClient::new(writer_id, manager.clone(), buffer);
    let mut expected = vec![];
    let wal = ChunkBasedWAL::load(storage)?;

    for i in 0..10 {
        expected.push(Bytes::from(serde_json::to_vec(&Foo { baz: i + 10 })?));
    }

    let stream_name = Uuid::new_v4().to_string();

    let result = writer_client
        .append(&stream_name, ExpectedRevision::Any, true, expected.clone())
        .await?;

    let result = match result {
        AppendStreamCompleted::Success(r) => r,
        AppendStreamCompleted::Error(e) => eyre::bail!("append_error: {:?}", e),
    };

    let mut index = 0usize;
    let mut stream = index_client
        .read(
            mikoshi_hash(&stream_name),
            0,
            usize::MAX,
            Direction::Forward,
        )
        .await?;

    while let Some((revision, position)) = stream.next().await? {
        assert_eq!(index as u64, revision);

        let log = wal.read_at(position)?;
        let foo = serde_json::from_slice::<Foo>(&log.payload)?;

        assert_eq!(foo.baz, index as u32 + 10);

        index += 1;
    }

    assert_eq!(expected.len(), index);

    Ok(())
}
