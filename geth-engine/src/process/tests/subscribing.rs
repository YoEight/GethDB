use crate::process::indexing::Indexing;
use crate::process::reading::{ReaderClient, Reading};
use crate::process::start_process_manager;
use crate::process::subscription::{PubSub, SubscriptionClient};
use crate::process::writing::{WriterClient, Writing};
use bytes::{Buf, Bytes, BytesMut};
use geth_common::{Direction, ExpectedRevision, Revision};
use geth_mikoshi::wal::chunks::ChunkContainer;
use geth_mikoshi::InMemoryStorage;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Foo {
    baz: u32,
}

#[tokio::test]
async fn test_pubsub_proc_simple() -> eyre::Result<()> {
    let _ = tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .init();

    let mut buffer = BytesMut::new();
    let storage = InMemoryStorage::new();
    let manager = start_process_manager(storage.clone());
    let container = ChunkContainer::load(storage.clone(), &mut buffer)?;
    let writer_id = manager.wait_for("writer").await?;
    let pubsub_id = manager.wait_for("pubsub").await?;
    let mut writer_client = WriterClient::new(writer_id, manager.clone(), buffer.split());
    let mut sub_client = SubscriptionClient::new(pubsub_id, manager.clone(), buffer);
    let mut expected = vec![];
    let stream_name = Uuid::new_v4().to_string();

    let mut stream = sub_client.subscribe(&stream_name).await?;

    for i in 0..10 {
        expected.push(Bytes::from(serde_json::to_vec(&Foo { baz: i + 10 })?));
    }

    let _ = writer_client
        .append(&stream_name, ExpectedRevision::Any, true, expected.clone())
        .await?
        .success()?;

    let mut count = 0;
    while let Some(mut entry) = stream.next().await {
        let revision = entry.payload.get_u64_le();
        assert_eq!(count, revision);

        let stream_name_len = entry.payload.get_u16_le() as usize;
        let stream_name_bytes = entry.payload.copy_to_bytes(stream_name_len);
        let actual_stream_name = unsafe { String::from_utf8_unchecked(stream_name_bytes.to_vec()) };
        assert_eq!(stream_name, actual_stream_name);

        let payload_len = entry.payload.get_u32_le() as usize;
        let payload = entry.payload.copy_to_bytes(payload_len);
        let foo = serde_json::from_slice::<Foo>(&payload)?;

        assert_eq!(foo.baz, count as u32 + 10);

        count += 1;

        if count >= 10 {
            break;
        }
    }

    assert_eq!(expected.len(), count as usize);

    Ok(())
}
