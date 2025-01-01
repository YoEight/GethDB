use crate::process::reading::ReaderClient;
use crate::process::writing::WriterClient;
use crate::process::{start_process_manager, Proc};
use crate::Options;
use bytes::{Buf, Bytes};
use geth_common::{Direction, ExpectedRevision, Revision};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Foo {
    baz: u32,
}

#[tokio::test]
async fn test_reader_proc_simple() -> eyre::Result<()> {
    let manager = start_process_manager(Options::in_mem()).await?;
    let writer_id = manager.wait_for(Proc::Writing).await?;
    let reader_id = manager.wait_for(Proc::Reading).await?;
    let writer_client = WriterClient::new(writer_id, manager.clone());
    let reader_client = ReaderClient::new(reader_id, manager.clone());
    let mut expected = vec![];

    for i in 0..10 {
        expected.push(Bytes::from(serde_json::to_vec(&Foo { baz: i + 10 })?));
    }

    let stream_name = Uuid::new_v4().to_string();

    let _ = writer_client
        .append(&stream_name, ExpectedRevision::Any, true, expected.clone())
        .await?
        .success()?;

    let mut stream = reader_client
        .read(
            &stream_name,
            Revision::Start,
            Direction::Forward,
            usize::MAX,
        )
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
    }

    assert_eq!(expected.len(), count as usize);

    Ok(())
}
