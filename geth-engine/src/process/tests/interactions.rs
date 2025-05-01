use crate::{
    process::{
        messages::TestSinkResponses, sink::SinkClient, start_process_manager_with_catalog, Catalog,
        Mail, Proc,
    },
    Options,
};
use bytes::{BufMut, BytesMut};

fn test_catalog() -> Catalog {
    Catalog::builder()
        .register(Proc::Echo)
        .register(Proc::Sink)
        .register(Proc::Panic)
        .build()
}

fn sink_response(resp: Mail) -> u64 {
    match resp.payload.try_into().ok() {
        Some(TestSinkResponses::Stream(value)) => value,
        _ => panic!("Unexpected response"),
    }
}

#[tokio::test]
async fn test_spawn_and_receive_mails() -> eyre::Result<()> {
    let mut buffer = BytesMut::new();
    let manager = start_process_manager_with_catalog(Options::in_mem(), test_catalog()).await?;
    let echo_proc_id = manager.wait_for(Proc::Echo).await?;

    let mut count = 0u64;
    while count < 10 {
        buffer.put_u64_le(count);

        let resp = manager
            .request(echo_proc_id, TestSinkResponses::Stream(count).into())
            .await?;

        assert_eq!(echo_proc_id, resp.origin);
        assert_eq!(count, sink_response(resp));

        count += 1;
    }

    Ok(())
}

#[tokio::test]
async fn test_streaming() -> eyre::Result<()> {
    let manager = start_process_manager_with_catalog(Options::in_mem(), test_catalog()).await?;
    let sink = SinkClient::resolve(manager.clone()).await?;
    let mut streaming = sink.stream_from(0, 10).await?;
    let mut count = 0u64;

    while let Some(value) = streaming.next().await? {
        assert_eq!(value, count);
        count += 1;
    }

    Ok(())
}

#[tokio::test]
async fn test_find_proc() -> eyre::Result<()> {
    let manager = start_process_manager_with_catalog(Options::in_mem(), test_catalog()).await?;
    let proc_id = manager.wait_for(Proc::Echo).await?;
    let find_proc_id = manager.find(Proc::Echo).await?;

    assert!(find_proc_id.is_some());
    assert_eq!(proc_id, find_proc_id.unwrap().id);

    Ok(())
}

#[tokio::test]
async fn test_shutdown_reported_properly() -> eyre::Result<()> {
    let manager = start_process_manager_with_catalog(Options::in_mem(), test_catalog()).await?;
    let proc_id = manager.wait_for(Proc::Echo).await?;

    let num = 42;
    let resp = manager
        .request(proc_id, TestSinkResponses::Stream(num).into())
        .await?;

    assert_eq!(proc_id, resp.origin);
    assert_eq!(num, sink_response(resp));

    manager.shutdown().await?;

    assert!(manager.wait_for(Proc::Echo).await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_request_returns_when_proc_panicked() -> eyre::Result<()> {
    let manager = start_process_manager_with_catalog(Options::in_mem(), test_catalog()).await?;
    let proc_id = manager.wait_for(Proc::Panic).await?;

    let resp = manager
        .request(proc_id, TestSinkResponses::Stream(42).into())
        .await?;

    assert!(resp.payload.is_fatal_error());

    Ok(())
}

#[tokio::test]
async fn test_stream_returns_when_proc_panicked() -> eyre::Result<()> {
    let manager = start_process_manager_with_catalog(Options::in_mem(), test_catalog()).await?;
    let proc_id = manager.wait_for(Proc::Panic).await?;

    let mut resp = manager
        .request_stream(proc_id, TestSinkResponses::Stream(42).into())
        .await?;

    assert!(resp.recv().await.is_none());

    Ok(())
}
