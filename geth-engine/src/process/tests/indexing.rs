use std::usize;

use geth_common::Direction;
use geth_domain::index::BlockEntry;
use geth_mikoshi::hashing::mikoshi_hash;
use uuid::Uuid;

use crate::{Options, RequestContext};

#[tokio::test]
async fn test_store_read() -> eyre::Result<()> {
    let embedded = crate::run_embedded(&Options::in_mem_no_grpc()).await?;
    let client = embedded.manager().new_index_client().await?;
    let ctx = RequestContext::new();
    let mut expected = vec![];

    for i in 0..10 {
        expected.push(BlockEntry {
            key: 2,
            revision: i,
            position: i + 10,
        });
    }

    client.store(ctx, expected.clone()).await?;
    let entries = client
        .read(ctx, 2, 0, usize::MAX, Direction::Forward)
        .await?
        .ok()?
        .collect()
        .await?;

    assert_eq!(expected, entries);

    embedded.shutdown().await
}

#[tokio::test]
async fn test_last_revision_when_exists() -> eyre::Result<()> {
    let embedded = crate::run_embedded(&Options::in_mem_no_grpc()).await?;
    let client = embedded.manager().new_index_client().await?;
    let ctx = RequestContext::new();
    let mut expected = vec![];

    for i in 0..10 {
        expected.push(BlockEntry {
            key: 2,
            revision: i,
            position: i + 10,
        });
    }

    client.store(ctx, expected.clone()).await?;
    let revision = client.latest_revision(ctx, 2).await?.revision();

    assert!(revision.is_some());
    assert_eq!(9, revision.unwrap());

    embedded.shutdown().await
}

#[tokio::test]
async fn test_last_revision_when_non_existent() -> eyre::Result<()> {
    let embedded = crate::run_embedded(&Options::in_mem_no_grpc()).await?;
    let client = embedded.manager().new_index_client().await?;
    let ctx = RequestContext::new();
    let revision = client.latest_revision(ctx, 2).await?.revision();

    assert!(revision.is_none());

    embedded.shutdown().await
}

#[tokio::test]
async fn test_empty_index_does_not_hang() -> eyre::Result<()> {
    let embedded = crate::run_embedded(&Options::in_mem_no_grpc()).await?;
    let client = embedded.manager().new_index_client().await?;
    let ctx = RequestContext::new();

    let stream_name = Uuid::new_v4().to_string();

    let mut streaming = client
        .read(
            ctx,
            mikoshi_hash(&stream_name),
            0,
            usize::MAX,
            Direction::Forward,
        )
        .await?
        .ok()?;

    while let Some(_) = streaming.next().await? {}

    Ok(())
}
