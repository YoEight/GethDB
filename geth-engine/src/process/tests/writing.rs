use crate::process::indexing::IndexClient;
use crate::process::reading::ReaderClient;
use crate::process::writing::WriterClient;
use crate::process::{start_process_manager, Proc};
use crate::Options;
use geth_common::{AppendStreamCompleted, Direction, ExpectedRevision, Propose, Record};
use geth_mikoshi::hashing::mikoshi_hash;
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
    let reader_id = manager.wait_for(Proc::Reading).await?;
    let mut index_client = IndexClient::new(proc_id, manager.clone());
    let writer_id = manager.wait_for(Proc::Writing).await?;
    let writer_client = WriterClient::new(writer_id, manager.clone());
    let reader_client = ReaderClient::new(reader_id, manager.clone());
    let mut expected = vec![];

    for i in 0..10 {
        expected.push(Propose::from_value(&Foo { baz: i + 10 })?);
    }

    let stream_name = Uuid::new_v4().to_string();

    let result = writer_client
        .append(stream_name.clone(), ExpectedRevision::Any, expected.clone())
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

    while let Some(entry) = stream.next().await? {
        assert_eq!(index as u64, entry.revision);

        let record: Record = reader_client.read_at(entry.position).await?.into();
        assert_eq!(index as u64, record.revision);
        assert_eq!(stream_name, record.stream_name);

        let foo = record.as_value::<Foo>()?;

        assert_eq!(foo.baz, index as u32 + 10);

        index += 1;
    }

    assert_eq!(expected.len(), index);

    Ok(())
}
