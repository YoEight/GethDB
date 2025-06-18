use geth_common::Direction;
use geth_domain::index::BlockEntry;

use crate::{process::start_process_manager, Options, RequestContext};

#[tokio::test]
async fn test_store_read() -> eyre::Result<()> {
    let manager = start_process_manager(Options::in_mem()).await?;
    let client = manager.new_index_client().await?;
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

    Ok(())
}

#[tokio::test]
async fn test_last_revision_when_exists() -> eyre::Result<()> {
    let manager = start_process_manager(Options::in_mem()).await?;
    let client = manager.new_index_client().await?;
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

    Ok(())
}

#[tokio::test]
async fn test_last_revision_when_non_existent() -> eyre::Result<()> {
    let manager = start_process_manager(Options::in_mem()).await?;
    let client = manager.new_index_client().await?;
    let ctx = RequestContext::new();
    let revision = client.latest_revision(ctx, 2).await?.revision();

    assert!(revision.is_none());

    Ok(())
}
