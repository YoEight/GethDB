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

#[tokio::test(flavor = "multi_thread")]
async fn test_spawn_and_receive_mails() -> eyre::Result<()> {
    let mut buffer = BytesMut::new();
    let mut mails = vec![];
    let correlation = Uuid::new_v4();

    for i in 0..10 {
        buffer.put_u64_le(i);

        mails.push(Mail {
            origin: Uuid::nil(),
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
