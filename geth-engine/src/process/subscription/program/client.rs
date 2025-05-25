use geth_common::{ProgramStats, ProgramSummary};
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

use crate::{
    process::{
        messages::{Messages, ProgramRequests, ProgramResponses},
        ProcId, ProcessEnv,
    },
    ManagerClient, Proc,
};

pub enum ProgramStartResult {
    Started,
    Failed(eyre::Report),
}

#[derive(Clone)]
pub struct ProgramClient {
    target: ProcId,
    inner: ManagerClient,
}

impl ProgramClient {
    pub fn new(target: ProcId, inner: ManagerClient) -> Self {
        Self { target, inner }
    }

    pub async fn start(
        &self,
        name: String,
        code: String,
        output: UnboundedSender<Messages>,
    ) -> eyre::Result<ProgramStartResult> {
        let mailbox = self
            .inner
            .request_opt(
                self.target,
                ProgramRequests::Start {
                    name,
                    code,
                    sender: output,
                }
                .into(),
            )
            .await?;

        let mailbox = if let Some(mailbox) = mailbox {
            mailbox
        } else {
            return Ok(ProgramStartResult::Failed(eyre::eyre!(
                "could not start a new program"
            )));
        };

        if let Some(resp) = mailbox.payload.try_into().ok() {
            match resp {
                ProgramResponses::Started => {
                    return Ok(ProgramStartResult::Started);
                }

                ProgramResponses::Error(e) => return Ok(ProgramStartResult::Failed(e)),

                _ => {
                    eyre::bail!("protocol error when communicating with the pyro-worker process");
                }
            }
        }

        eyre::bail!("protocol error when communicating with the pyro-worker process");
    }

    pub async fn stats(&self) -> eyre::Result<Option<ProgramStats>> {
        let mailbox = self
            .inner
            .request_opt(
                self.target,
                ProgramRequests::Stats { id: Uuid::nil() }.into(),
            )
            .await?;

        let mailbox = if let Some(mailbox) = mailbox {
            mailbox
        } else {
            return Ok(None);
        };

        if let Some(resp) = mailbox.payload.try_into().ok() {
            match resp {
                ProgramResponses::Stats(stats) => {
                    return Ok(Some(stats));
                }

                _ => {
                    eyre::bail!("protocol error when communicating with the pyro-worker process");
                }
            }
        }

        eyre::bail!("protocol error when communicating with the pyro-worker process");
    }

    pub async fn summary(&self) -> eyre::Result<Option<ProgramSummary>> {
        let mailbox = self
            .inner
            .request_opt(self.target, ProgramRequests::Get { id: Uuid::nil() }.into())
            .await?;

        let mailbox = if let Some(mailbox) = mailbox {
            mailbox
        } else {
            return Ok(None);
        };

        if let Some(resp) = mailbox.payload.try_into().ok() {
            match resp {
                ProgramResponses::Get(summary) => {
                    return Ok(Some(summary));
                }

                _ => {
                    eyre::bail!("protocol error when communicating with the pyro-worker process");
                }
            }
        }

        eyre::bail!("protocol error when communicating with the pyro-worker process");
    }

    pub async fn stop(self) -> eyre::Result<()> {
        let mailbox = self
            .inner
            .request_opt(
                self.target,
                ProgramRequests::Stop { id: Uuid::nil() }.into(),
            )
            .await?;

        let mailbox = if let Some(mailbox) = mailbox {
            mailbox
        } else {
            return Ok(());
        };

        if let Some(resp) = mailbox.payload.try_into().ok() {
            match resp {
                ProgramResponses::Stopped => {
                    return Ok(());
                }

                _ => {
                    eyre::bail!("protocol error when communicating with the pyro-worker process");
                }
            }
        }

        eyre::bail!("protocol error when communicating with the pyro-worker process");
    }
}
