use geth_common::Direction;
use geth_mikoshi::InMemoryStorage;

use crate::{
    process::{indexing::IndexClient, start_process_manager, Proc},
    Options,
};

#[tokio::test]
async fn test_store_read() -> eyre::Result<()> {
    let storage = InMemoryStorage::new();
    let manager = start_process_manager(Options::in_mem()).await?;
    let proc_id = manager.wait_for(Proc::Indexing).await?;
    let mut client = IndexClient::new(proc_id, manager.clone());
    let mut expected = vec![];

    for i in 0..10 {
        expected.push((i, i + 10));
    }

    client.store(2, expected.clone()).await?;
    let entries = client
        .read(2, 0, usize::MAX, Direction::Forward)
        .await?
        .ok()?
        .collect()
        .await?;

    assert_eq!(expected, entries);

    Ok(())
}

#[tokio::test]
async fn test_last_revision_when_exists() -> eyre::Result<()> {
    let storage = InMemoryStorage::new();
    let manager = start_process_manager(Options::in_mem()).await?;
    let proc_id = manager.wait_for(Proc::Indexing).await?;
    let mut client = IndexClient::new(proc_id, manager.clone());
    let mut expected = vec![];

    for i in 0..10 {
        expected.push((i, i + 10));
    }

    client.store(2, expected.clone()).await?;
    let revision = client.latest_revision(2).await?.revision();

    assert!(revision.is_some());
    assert_eq!(9, revision.unwrap());

    Ok(())
}

#[tokio::test]
async fn test_last_revision_when_non_existent() -> eyre::Result<()> {
    let storage = InMemoryStorage::new();
    let manager = start_process_manager(Options::in_mem()).await?;
    let proc_id = manager.wait_for(Proc::Indexing).await?;
    let mut client = IndexClient::new(proc_id, manager.clone());
    let revision = client.latest_revision(2).await?.revision();

    assert!(revision.is_none());

    Ok(())
}
