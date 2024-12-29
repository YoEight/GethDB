use crate::process::resource::BufferManager;
use crate::process::subscription::{PushBuilder, Request, Response};
use crate::process::{ManagerClient, Proc, ProcId, ProcessEnv, ProcessRawEnv};
use bb8::Pool;
use bytes::{Buf, Bytes};
use geth_mikoshi::wal::LogEntry;
use tokio::sync::mpsc::UnboundedReceiver;

pub struct Streaming {
    inner: UnboundedReceiver<Bytes>,
}

impl Streaming {
    pub fn from(inner: UnboundedReceiver<Bytes>) -> Self {
        Self { inner }
    }

    pub async fn next(&mut self) -> Option<LogEntry> {
        if let Some(mut bytes) = self.inner.recv().await {
            return Some(LogEntry {
                position: bytes.get_u64_le(),
                r#type: bytes.get_u8(),
                payload: bytes,
            });
        }

        None
    }
}

#[derive(Clone)]
pub struct SubscriptionClient {
    target: ProcId,
    inner: ManagerClient,
}

impl SubscriptionClient {
    pub fn new(target: ProcId, inner: ManagerClient) -> Self {
        Self { target, inner }
    }

    pub async fn resolve(env: &ProcessEnv) -> eyre::Result<Self> {
        tracing::debug!("waiting for the pubsub process to be available...");
        let proc_id = env.client.wait_for(Proc::PubSub).await?;
        tracing::debug!("pubsub process available on {}", proc_id);
        Ok(Self::new(proc_id, env.client.clone()))
    }
    pub fn resolve_raw(env: &ProcessRawEnv) -> eyre::Result<Self> {
        tracing::debug!("waiting for the pubsub process to be available...");
        let proc_id = env.handle.block_on(env.client.wait_for(Proc::PubSub))?;
        tracing::debug!("pubsub process available on {}", proc_id);
        Ok(Self::new(proc_id, env.client.clone()))
    }

    pub fn push(&mut self, builder: PushBuilder<'_>) -> eyre::Result<()> {
        self.inner.send(self.target, builder.build())?;

        Ok(())
    }

    pub async fn subscribe(&self, stream_name: &str) -> eyre::Result<Streaming> {
        let mut buffer = self.inner.pool.get().await.unwrap();
        let mut mailbox = self
            .inner
            .request_stream(self.target, Request::subscribe(&mut buffer, stream_name))
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
