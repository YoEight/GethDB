use crate::process::reading::record_try_from;
use crate::process::start_process_manager;
use crate::process::tests::Foo;
use crate::Options;
use geth_common::{AppendStreamCompleted, Direction, ExpectedRevision, Propose, Record};
use geth_mikoshi::hashing::mikoshi_hash;
use uuid::Uuid;

#[tokio::test]
async fn test_writer_proc_simple() -> eyre::Result<()> {
    let manager = start_process_manager(Options::in_mem()).await?;
    let index_client = manager.new_index_client().await?;
    let writer_client = manager.new_writer_client().await?;
    let reader_client = manager.new_reader_client().await?;
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

        let record: Record = record_try_from(reader_client.read_at(entry.position).await?)?;
        assert_eq!(index as u64, record.revision);
        assert_eq!(stream_name, record.stream_name);

        let foo = record.as_value::<Foo>()?;

        assert_eq!(foo.baz, index as u32 + 10);

        index += 1;
    }

    assert_eq!(expected.len(), index);

    Ok(())
}
