use crate::process::messages::{Messages, SubscribeRequests, SubscribeResponses};
use crate::process::{ManagerClient, Proc, ProcId, ProcessEnv, ProcessRawEnv};
use geth_mikoshi::wal::LogEntry;
use tokio::sync::mpsc::UnboundedReceiver;

pub struct Streaming {
    inner: UnboundedReceiver<Messages>,
}

impl Streaming {
    pub fn from(inner: UnboundedReceiver<Messages>) -> Self {
        Self { inner }
    }

    pub async fn next(&mut self) -> eyre::Result<Option<LogEntry>> {
        if let Some(resp) = self.inner.recv().await.and_then(|r| r.try_from().ok()) {
            match resp {
                SubscribeResponses::Error => {
                    eyre::bail!("error when streaming from the pubsub process");
                }

                SubscribeResponses::Entry(entry) => {
                    return Ok(Some(entry));
                }

                _ => {
                    eyre::bail!("unexpected message when streaming from the pubsub process");
                }
            }
        }

        Ok(None)
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

    pub async fn subscribe(&self, stream_name: &str) -> eyre::Result<Streaming> {
        let mut mailbox = self
            .inner
            .request_stream(
                self.target,
                SubscribeRequests::Subscribe {
                    ident: stream_name.to_string(),
                }
                .into(),
            )
            .await?;

        if let Some(resp) = mailbox.recv().await.and_then(|r| r.try_into().ok()) {
            match resp {
                SubscribeResponses::Error => {
                    eyre::bail!("internal error");
                }

                SubscribeResponses::Confirmed => {
                    return Ok(Streaming::from(mailbox));
                }

                _ => {
                    eyre::bail!("protocol error when communicating with the pubsub process");
                }
            }
        }

        eyre::bail!("pubsub process is no longer running")
    }

    pub async fn push(&self, events: Vec<LogEntry>) -> eyre::Result<()> {
        let resp = self
            .inner
            .request(self.target, SubscribeRequests::Push { events }.into())
            .await?;

        if let Some(resp) = resp.payload.try_into().ok() {
            match resp {
                SubscribeResponses::Error => {
                    eyre::bail!("internal error");
                }

                SubscribeResponses::Confirmed => {
                    return Ok(());
                }

                _ => {
                    eyre::bail!("protocol error when communicating with the pubsub process");
                }
            }
        }

        eyre::bail!("unexpected response from the pubsub process")
    }
}
