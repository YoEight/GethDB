use crate::process::messages::{
    Messages, ProgramRequests, ProgramResponses, SubscribeRequests, SubscribeResponses,
    SubscriptionType,
};
use crate::process::{ManagerClient, Proc, ProcId, ProcessEnv, ProcessRawEnv};
use geth_common::{ProgramStats, ProgramSummary, Record};
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::instrument;

pub struct Streaming {
    inner: UnboundedReceiver<Messages>,
}

impl Streaming {
    pub fn from(inner: UnboundedReceiver<Messages>) -> Self {
        Self { inner }
    }

    pub async fn next(&mut self) -> eyre::Result<Option<Record>> {
        if let Some(resp) = self.inner.recv().await.and_then(|r| r.try_into().ok()) {
            match resp {
                SubscribeResponses::Error(e) => {
                    return Err(e);
                }

                SubscribeResponses::Record(record) => {
                    return Ok(Some(record));
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
        let proc_id = env.client.wait_for(Proc::PubSub).await?.must_succeed()?;
        Ok(Self::new(proc_id, env.client.clone()))
    }

    pub fn resolve_raw(env: &ProcessRawEnv) -> eyre::Result<Self> {
        let proc_id = env
            .handle
            .block_on(env.client.wait_for(Proc::PubSub))?
            .must_succeed()?;
        Ok(Self::new(proc_id, env.client.clone()))
    }

    pub async fn subscribe_to_stream(&self, stream_name: &str) -> eyre::Result<Streaming> {
        let mut mailbox = self
            .inner
            .request_stream(
                self.target,
                SubscribeRequests::Subscribe(SubscriptionType::Stream {
                    ident: stream_name.to_string(),
                })
                .into(),
            )
            .await?;

        if let Some(resp) = mailbox.recv().await.and_then(|r| r.try_into().ok()) {
            match resp {
                SubscribeResponses::Error(e) => {
                    return Err(e);
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

    pub async fn subscribe_to_program(&self, name: &str, code: &str) -> eyre::Result<Streaming> {
        let mut mailbox = self
            .inner
            .request_stream(
                self.target,
                SubscribeRequests::Subscribe(SubscriptionType::Program {
                    name: name.to_string(),
                    code: code.to_string(),
                })
                .into(),
            )
            .await?;

        if let Some(resp) = mailbox.recv().await.and_then(|r| r.try_into().ok()) {
            match resp {
                SubscribeResponses::Error(e) => {
                    return Err(e);
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

    pub async fn list_programs(&self) -> eyre::Result<Vec<ProgramSummary>> {
        let mailbox = self
            .inner
            .request(
                self.target,
                SubscribeRequests::Program(ProgramRequests::List).into(),
            )
            .await?;

        if let Ok(resp) = mailbox.payload.try_into() {
            match resp {
                SubscribeResponses::Error(e) => {
                    return Err(e);
                }

                SubscribeResponses::Programs(ProgramResponses::List(list)) => {
                    return Ok(list);
                }

                _ => {
                    eyre::bail!("protocol error when communicating with the pubsub process");
                }
            }
        }

        eyre::bail!("pubsub sent an unexpected response")
    }

    pub async fn program_stats(&self, id: ProcId) -> eyre::Result<Option<ProgramStats>> {
        let mailbox = self
            .inner
            .request(
                self.target,
                SubscribeRequests::Program(ProgramRequests::Stats { id }).into(),
            )
            .await?;

        if let Ok(resp) = mailbox.payload.try_into() {
            match resp {
                SubscribeResponses::Error(e) => {
                    return Err(e);
                }

                SubscribeResponses::Programs(ProgramResponses::Stats(resp)) => {
                    return Ok(Some(resp));
                }

                SubscribeResponses::Programs(ProgramResponses::NotFound) => {
                    return Ok(None);
                }

                _ => {
                    eyre::bail!("protocol error when communicating with the pubsub process");
                }
            }
        }

        eyre::bail!("pubsub process is no longer running")
    }

    #[instrument(skip(self, events), fields(origin = ?self.inner.origin_proc))]
    pub async fn push(&self, events: Vec<Record>) -> eyre::Result<()> {
        tracing::debug!("sending push request to pubsub process {}", self.target);

        let resp = self
            .inner
            .request(self.target, SubscribeRequests::Push { events }.into())
            .await?;

        if let Ok(resp) = resp.payload.try_into() {
            match resp {
                SubscribeResponses::Error(e) => {
                    return Err(e);
                }

                SubscribeResponses::Confirmed => {
                    tracing::debug!("push request confirmed by the pubsub process");
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
