use std::usize;

use crate::Options;
use crate::RequestContext;
use geth_common::{Direction, ExpectedRevision, Propose, Revision};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Foo {
    baz: u32,
}

#[tokio::test]
async fn test_reader_proc_simple() -> eyre::Result<()> {
    let embedded = crate::run_embedded(&Options::in_mem_no_grpc()).await?;
    let writer_client = embedded.manager().new_writer_client().await?;
    let reader_client = embedded.manager().new_reader_client().await?;
    let ctx = RequestContext::new();
    let mut expected = vec![];

    for i in 0..10 {
        expected.push(Propose::from_value(&Foo { baz: i + 10 })?);
    }

    let stream_name = Uuid::new_v4().to_string();

    let _ = writer_client
        .append(
            ctx,
            stream_name.clone(),
            ExpectedRevision::Any,
            expected.clone(),
        )
        .await?
        .success()?;

    let mut stream = reader_client
        .read(
            ctx,
            &stream_name,
            Revision::Start,
            Direction::Forward,
            usize::MAX,
        )
        .await?
        .success()?;

    let mut count = 0;
    while let Some(record) = stream.next().await? {
        let foo = record.as_value::<Foo>()?;
        assert_eq!(count, record.revision);
        assert_eq!(stream_name, record.stream_name);
        assert_eq!(foo.baz, count as u32 + 10);

        count += 1;
    }

    assert_eq!(expected.len(), count as usize);

    embedded.shutdown().await
}

#[tokio::test]
async fn test_empty_read_does_not_hang() -> eyre::Result<()> {
    let embedded = crate::run_embedded(&Options::in_mem_no_grpc()).await?;
    let client = embedded.manager().new_reader_client().await?;
    let ctx = RequestContext::new();
    let stream_name = Uuid::new_v4().to_string();

    let mut streaming = client
        .read(
            ctx,
            &stream_name,
            Revision::Start,
            Direction::Forward,
            usize::MAX,
        )
        .await?
        .success()?;

    while let Some(_) = streaming.next().await? {}

    embedded.shutdown().await
}
