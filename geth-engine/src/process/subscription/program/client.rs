use tokio::sync::mpsc::UnboundedSender;

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

    pub async fn resolve(env: &ProcessEnv) -> eyre::Result<Self> {
        let proc_id = env.client.wait_for(Proc::PyroWorker).await?;
        Ok(Self::new(proc_id, env.client.clone()))
    }

    pub async fn start(
        &self,
        name: String,
        code: String,
        output: UnboundedSender<Messages>,
    ) -> eyre::Result<ProgramStartResult> {
        let mailbox = self
            .inner
            .request(
                self.target,
                ProgramRequests::Start {
                    name,
                    code,
                    sender: output,
                }
                .into(),
            )
            .await?;

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

        eyre::bail!("prog process is no longer running")
    }
}
