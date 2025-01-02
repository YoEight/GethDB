use crate::process::indexing::IndexClient;
use crate::process::writing::WriterClient;
use crate::process::{start_process_manager, Proc};
use crate::Options;
use bytes::{Buf, Bytes};
use geth_common::{AppendStreamCompleted, Direction, ExpectedRevision};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::wal::LogReader;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Foo {
    baz: u32,
}

#[tokio::test]
async fn test_writer_proc_simple() -> eyre::Result<()> {
    let manager = start_process_manager(Options::in_mem()).await?;
    let proc_id = manager.wait_for(Proc::Indexing).await?;
    let mut index_client = IndexClient::new(proc_id, manager.clone());
    let writer_id = manager.wait_for(Proc::Writing).await?;
    let writer_client = WriterClient::new(writer_id, manager.clone());
    let mut expected = vec![];
    // let wal = LogReader::new(container);

    for i in 0..10 {
        expected.push(Bytes::from(serde_json::to_vec(&Foo { baz: i + 10 })?));
    }

    let stream_name = Uuid::new_v4().to_string();

    let result = writer_client
        .append(&stream_name, ExpectedRevision::Any, true, expected.clone())
        .await?;

    if let AppendStreamCompleted::Error(e) = result {
        eyre::bail!("append_error: {:?}", e);
    };

    let mut index = 0usize;
    let mut stream = index_client
        .read(
            mikoshi_hash(&stream_name),
            0,
            usize::MAX,
            Direction::Forward,
        )
        .await?
        .ok()?;

    while let Some((revision, position)) = stream.next().await? {
        assert_eq!(index as u64, revision);

        let mut log = wal.read_at(position)?;
        let entry_revision = log.payload.get_u64_le();
        assert_eq!(revision, entry_revision);

        let stream_name_len = log.payload.get_u16_le() as usize;
        let stream_name_bytes = log.payload.copy_to_bytes(stream_name_len);
        let actual_stream_name = unsafe { String::from_utf8_unchecked(stream_name_bytes.to_vec()) };
        assert_eq!(stream_name, actual_stream_name);

        let payload_len = log.payload.get_u32_le() as usize;
        let payload = log.payload.copy_to_bytes(payload_len);
        let foo = serde_json::from_slice::<Foo>(&payload)?;

        assert_eq!(foo.baz, index as u32 + 10);

        index += 1;
    }

    assert_eq!(expected.len(), index);

    Ok(())
}
