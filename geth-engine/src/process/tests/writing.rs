use crate::process::tests::Foo;
use crate::Options;
use crate::{process::reading::record_try_from, RequestContext};
use geth_common::{AppendStreamCompleted, Direction, ExpectedRevision, Propose, Record};
use geth_mikoshi::hashing::mikoshi_hash;
use uuid::Uuid;

#[tokio::test]
async fn test_writer_proc_simple() -> eyre::Result<()> {
    let embedded = crate::run_embedded(&Options::in_mem_no_grpc()).await?;
    let index_client = embedded.manager().new_index_client().await?;
    let writer_client = embedded.manager().new_writer_client().await?;
    let reader_client = embedded.manager().new_reader_client().await?;
    let ctx = RequestContext::new();
    let mut expected = vec![];

    for i in 0..10 {
        expected.push(Propose::from_value(&Foo { baz: i + 10 })?);
    }

    let stream_name = Uuid::new_v4().to_string();

    let result = writer_client
        .append(
            ctx,
            stream_name.clone(),
            ExpectedRevision::Any,
            expected.clone(),
        )
        .await?;

    if let AppendStreamCompleted::Error(e) = result {
        eyre::bail!("append_error: {:?}", e);
    };

    let mut index = 0usize;
    let mut stream = index_client
        .read(
            ctx,
            mikoshi_hash(&stream_name),
            0,
            usize::MAX,
            Direction::Forward,
        )
        .await?
        .ok()?;

    while let Some(entry) = stream.next().await? {
        assert_eq!(index as u64, entry.revision);

        let record: Record = record_try_from(reader_client.read_at(ctx, entry.position).await?)?;
        assert_eq!(index as u64, record.revision);
        assert_eq!(stream_name, record.stream_name);

        let foo = record.as_value::<Foo>()?;

        assert_eq!(foo.baz, index as u32 + 10);

        index += 1;
    }

    assert_eq!(expected.len(), index);

    embedded.shutdown().await
}
