use crate::process::reading::Streaming;
use crate::process::subscription::{PushBuilder, Request, Response};
use crate::process::{reading, ManagerClient, ProcId, ProcessEnv, ProcessRawEnv};
use bytes::BytesMut;

pub struct SubscriptionClient {
    target: ProcId,
    inner: ManagerClient,
    buffer: BytesMut,
}

impl SubscriptionClient {
    pub fn new(target: ProcId, inner: ManagerClient, buffer: BytesMut) -> Self {
        Self {
            target,
            inner,
            buffer,
        }
    }

    pub async fn resolve(env: &mut ProcessEnv) -> eyre::Result<Self> {
        let proc_id = env.client.wait_for("subscription").await?;
        Ok(Self::new(proc_id, env.client.clone(), env.buffer.split()))
    }
    pub fn resolve_raw(env: &mut ProcessRawEnv) -> eyre::Result<Self> {
        let proc_id = env.handle.block_on(env.client.wait_for("index"))?;

        Ok(Self::new(proc_id, env.client.clone(), env.buffer.split()))
    }

    pub fn push(&mut self, builder: PushBuilder<'_>) -> eyre::Result<()> {
        self.inner.send(self.target, builder.build())?;

        Ok(())
    }

    pub async fn subscribe(&mut self, stream_name: &str) -> eyre::Result<reading::Streaming> {
        let mut mailbox = self
            .inner
            .request_stream(
                self.target,
                Request::subscribe(&mut self.buffer, stream_name),
            )
            .await?;

        if let Some(resp) = mailbox.recv().await {
            if let Some(resp) = Response::try_from(resp) {
                match resp {
                    Response::Error => {
                        eyre::bail!("internal error");
                    }

                    Response::Confirmed => {
                        return Ok(Streaming::from(mailbox));
                    }
                }
            }

            eyre::bail!("protocol error when communicating with the pubsub process");
        }

        eyre::bail!("pubsub process is no longer running")
    }
}
