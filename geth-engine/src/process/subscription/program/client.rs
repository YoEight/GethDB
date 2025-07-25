use geth_common::ProgramStats;
use tokio::sync::mpsc::UnboundedSender;
use tracing::instrument;

use crate::{
    process::{
        messages::{Messages, ProgramRequests, ProgramResponses},
        ProcId, RequestContext,
    },
    ManagerClient,
};

pub enum ProgramStartResult {
    Started,
    Failed(eyre::Report),
}

#[derive(Clone, Debug)]
pub struct ProgramClient {
    target: ProcId,
    inner: ManagerClient,
}

impl ProgramClient {
    pub fn new(target: ProcId, inner: ManagerClient) -> Self {
        Self { target, inner }
    }

    pub fn id(&self) -> ProcId {
        self.target
    }

    #[instrument(skip_all, fields(correlation = %context.correlation))]
    pub async fn start(
        &self,
        context: RequestContext,
        name: String,
        code: String,
        output: UnboundedSender<Messages>,
    ) -> eyre::Result<ProgramStartResult> {
        let mailbox = self
            .inner
            .request_opt(
                context,
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

        if let Ok(resp) = mailbox.payload.try_into() {
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

    #[instrument(skip_all, fields(correlation = %context.correlation))]
    pub async fn stats(&self, context: RequestContext) -> eyre::Result<Option<ProgramStats>> {
        let mailbox = self
            .inner
            .request_opt(
                context,
                self.target,
                ProgramRequests::Stats { id: 0 }.into(),
            )
            .await?;

        let mailbox = if let Some(mailbox) = mailbox {
            mailbox
        } else {
            return Ok(None);
        };

        if let Ok(resp) = mailbox.payload.try_into() {
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

    #[instrument(skip_all, fields(correlation = %context.correlation))]
    pub async fn stop(self, context: RequestContext) -> eyre::Result<()> {
        let mailbox = self
            .inner
            .request_opt(context, self.target, ProgramRequests::Stop { id: 0 }.into())
            .await?;

        let mailbox = if let Some(mailbox) = mailbox {
            mailbox
        } else {
            return Ok(());
        };

        if let Ok(resp) = mailbox.payload.try_into() {
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
