use crate::process::{start_process_manager_with_catalog, Catalog, Mail, Proc};
use bytes::{Buf, BufMut, BytesMut};
use geth_mikoshi::InMemoryStorage;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

fn test_catalog() -> Catalog {
    Catalog::builder()
        .register(Proc::Echo)
        .register(Proc::Sink)
        .build()
}

struct Sink {
    target: &'static str,
    sender: UnboundedSender<Mail>,
    mails: Vec<Mail>,
}

#[tokio::test]
async fn test_spawn_and_receive_mails() -> eyre::Result<()> {
    let mut buffer = BytesMut::new();
    let manager =
        start_process_manager_with_catalog(InMemoryStorage::new(), test_catalog()).await?;
    let echo_proc_id = manager.wait_for(Proc::Echo).await?;

    let mut count = 0u64;
    while count < 10 {
        buffer.put_u64_le(count);

        let mut resp = manager
            .request(echo_proc_id, buffer.split().freeze())
            .await?;

        assert_eq!(echo_proc_id, resp.origin);
        assert_eq!(count, resp.payload.get_u64_le());

        count += 1;
    }

    Ok(())
}

#[tokio::test]
async fn test_find_proc() -> eyre::Result<()> {
    let manager =
        start_process_manager_with_catalog(InMemoryStorage::new(), test_catalog()).await?;
    let proc_id = manager.wait_for(Proc::Echo).await?;
    let find_proc_id = manager.find(Proc::Echo).await?;

    assert!(find_proc_id.is_some());
    assert_eq!(proc_id, find_proc_id.unwrap());

    Ok(())
}

#[tokio::test]
async fn test_simple_request() -> eyre::Result<()> {
    let mut buffer = BytesMut::new();
    let manager =
        start_process_manager_with_catalog(InMemoryStorage::new(), test_catalog()).await?;
    let proc_id = manager.wait_for(Proc::Echo).await?;

    let random_uuid = Uuid::new_v4();
    buffer.put_u128_le(random_uuid.to_u128_le());
    let mut resp = manager.request(proc_id, buffer.split().freeze()).await?;

    assert_eq!(proc_id, resp.origin);
    assert_eq!(random_uuid, Uuid::from_u128_le(resp.payload.get_u128_le()));

    Ok(())
}

#[tokio::test]
async fn test_shutdown_reported_properly() -> eyre::Result<()> {
    let mut buffer = BytesMut::new();
    let manager =
        start_process_manager_with_catalog(InMemoryStorage::new(), test_catalog()).await?;
    let proc_id = manager.wait_for(Proc::Echo).await?;

    let random_uuid = Uuid::new_v4();
    buffer.put_u128_le(random_uuid.to_u128_le());
    let mut resp = manager.request(proc_id, buffer.split().freeze()).await?;

    assert_eq!(proc_id, resp.origin);
    assert_eq!(random_uuid, Uuid::from_u128_le(resp.payload.get_u128_le()));

    manager.shutdown().await?;

    assert!(manager.wait_for(Proc::Echo).await.is_err());

    Ok(())
}
