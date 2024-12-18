use crate::process::{start_process_manager, Item, Mail, ProcessEnv, Runnable};
use bytes::{Buf, BufMut, BytesMut};
use std::time::Instant;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

struct EchoProc;

#[async_trait::async_trait]
impl Runnable for EchoProc {
    fn name(&self) -> &'static str {
        "echo"
    }

    async fn run(self: Box<Self>, mut env: ProcessEnv) -> eyre::Result<()> {
        while let Some(item) = env.queue.recv().await {
            if let Item::Mail(mail) = item {
                env.client
                    .reply(mail.origin, mail.correlation, mail.payload)?;
            }
        }

        Ok(())
    }
}

struct Sink {
    target: &'static str,
    sender: UnboundedSender<Mail>,
    mails: Vec<Mail>,
}

#[async_trait::async_trait]
impl Runnable for Sink {
    fn name(&self) -> &'static str {
        "sink"
    }

    async fn run(self: Box<Self>, mut env: ProcessEnv) -> eyre::Result<()> {
        let proc_id = env.client.wait_for(self.target).await?;

        for mail in self.mails {
            env.client
                .send_with_correlation(proc_id, mail.correlation, mail.payload)?;
        }

        while let Some(item) = env.queue.recv().await {
            if let Item::Mail(mail) = item {
                let _ = self.sender.send(mail);
            }
        }

        Ok(())
    }
}

struct StreamerProc;

#[async_trait::async_trait]
impl Runnable for StreamerProc {
    fn name(&self) -> &'static str {
        "streamer"
    }

    async fn run(self: Box<Self>, mut env: ProcessEnv) -> eyre::Result<()> {
        while let Some(item) = env.queue.recv().await {
            if let Item::Stream(mut stream) = item {
                while stream.payload.has_remaining() {
                    let value = stream.payload.get_u64_le();
                    env.buffer.put_u64_le(value);

                    if stream.sender.send(env.buffer.split().freeze()).is_err() {
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

#[tokio::test]
async fn test_spawn_and_receive_mails() -> eyre::Result<()> {
    let mut buffer = BytesMut::new();
    let mut mails = vec![];
    let correlation = Uuid::new_v4();

    for i in 0..10 {
        buffer.put_u64_le(i);

        mails.push(Mail {
            origin: 0,
            correlation,
            payload: buffer.split().freeze(),
            created: Instant::now(),
        });
    }

    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let manager = start_process_manager();

    let echo_proc_id = manager.spawn(EchoProc).await?;
    manager
        .spawn(Sink {
            target: "echo",
            sender,
            mails,
        })
        .await?;

    let mut count = 0u64;
    while count < 10 {
        let mut mail = receiver.recv().await.unwrap();

        assert_eq!(echo_proc_id, mail.origin);
        assert_eq!(mail.correlation, correlation);
        assert_eq!(count, mail.payload.get_u64_le());

        count += 1;
    }

    Ok(())
}

#[tokio::test]
async fn test_find_proc() -> eyre::Result<()> {
    let manager = start_process_manager();
    let proc_id = manager.spawn(EchoProc).await?;
    let find_proc_id = manager.find("echo").await?;

    assert!(find_proc_id.is_some());
    assert_eq!(proc_id, find_proc_id.unwrap());

    Ok(())
}

#[tokio::test]
async fn test_simple_request() -> eyre::Result<()> {
    let mut buffer = BytesMut::new();
    let manager = start_process_manager();
    let proc_id = manager.spawn(EchoProc).await?;

    let random_uuid = Uuid::new_v4();
    buffer.put_u128_le(random_uuid.to_u128_le());
    let mut resp = manager.request(proc_id, buffer.split().freeze()).await?;

    assert_eq!(proc_id, resp.origin);
    assert_eq!(random_uuid, Uuid::from_u128_le(resp.payload.get_u128_le()));

    Ok(())
}

#[tokio::test]
async fn test_simple_streaming() -> eyre::Result<()> {
    let mut buffer = BytesMut::new();
    let manager = start_process_manager();
    let proc_id = manager.spawn(StreamerProc).await?;
    let mut input = vec![];

    for i in 0..10 {
        buffer.put_u64_le(i);
        input.push(i);
    }

    let mut resp = manager
        .request_stream(proc_id, buffer.split().freeze())
        .await?;

    let mut output = vec![];

    while let Some(mut msg) = resp.recv().await {
        output.push(msg.get_u64_le());
    }

    assert_eq!(input.len(), output.len());
    assert_eq!(input, output);

    Ok(())
}

#[tokio::test]
async fn test_shutdown_reported_properly() -> eyre::Result<()> {
    let mut buffer = BytesMut::new();
    let manager = start_process_manager();
    let proc_id = manager.spawn(EchoProc).await?;

    let random_uuid = Uuid::new_v4();
    buffer.put_u128_le(random_uuid.to_u128_le());
    let mut resp = manager.request(proc_id, buffer.split().freeze()).await?;

    assert_eq!(proc_id, resp.origin);
    assert_eq!(random_uuid, Uuid::from_u128_le(resp.payload.get_u128_le()));

    manager.shutdown().await?;

    assert!(manager.spawn(EchoProc).await.is_err());

    Ok(())
}
