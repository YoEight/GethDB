use crate::process::reading::ReaderClient;
use crate::process::writing::WriterClient;
use crate::process::{start_process_manager, Proc};
use crate::Options;
use geth_common::{Direction, ExpectedRevision, Propose, Record, Revision};
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
        expected.push(Propose::from_value(&Foo { baz: i + 10 })?);
    }

    let stream_name = Uuid::new_v4().to_string();

    let _ = writer_client
        .append(stream_name.clone(), ExpectedRevision::Any, expected.clone())
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
    while let Some(entry) = stream.next().await? {
        let record: Record = entry.clone().into();
        let foo = record.as_value::<Foo>()?;
        assert_eq!(count, record.revision);
        assert_eq!(stream_name, record.stream_name);
        assert_eq!(foo.baz, count as u32 + 10);

        count += 1;
    }

    assert_eq!(expected.len(), count as usize);

    Ok(())
}
