use std::{future::Future, pin::Pin, sync::Arc, thread};

use tokio::sync::{mpsc::unbounded_channel, oneshot};
use uuid::Uuid;

use crate::{
    process::{manager::ManagerClient, Mailbox, Managed, ProcessEnv, Raw, RunningProc},
    Options, Proc,
};

pub struct SpawnParams {
    pub options: Options,
    pub client: ManagerClient,
    pub process: Process,
}

type ManagedProcessFn =
    Box<dyn Fn(ProcessEnv<Managed>) -> Pin<Box<dyn Future<Output = eyre::Result<()>>>> + Send>;

type ManagedProcess = Arc<dyn Fn() -> ManagedProcessFn>;

type RawProcessFn = Box<dyn Fn(ProcessEnv<Raw>) -> eyre::Result<()> + Send>;

type RawProcess = Arc<dyn Fn() -> RawProcessFn>;

#[derive(Clone)]
pub struct Process {
    pub proc: Proc,
    pub run: Run,
}

impl Process {
    pub fn managed<F, Fut>(proc: Proc, run: F) -> Self
    where
        F: Fn(ProcessEnv<Managed>) -> Fut,
        Fut: Future<Output = eyre::Result<()>>,
    {
        Self {
            proc,
            run: Run::Managed(Arc::new(move || Box::new(|x| Box::pin(run(x))))),
        }
    }

    pub fn raw<F>(proc: Proc, run: F) -> Self
    where
        F: Fn(ProcessEnv<Managed>) -> eyre::Result<()>,
    {
        Self {
            proc,
            run: Run::Raw(Arc::new(move || Box::new(run))),
        }
    }
}

#[derive(Clone)]
pub enum Run {
    Managed(ManagedProcess),
    Raw(RawProcess),
}

pub fn spawn_process(params: SpawnParams) {
    tokio::spawn(async move {
        let id = params.client.id();
        let client = params.client.clone();
        let (sender_ready, recv_ready) = oneshot::channel();

        let mailbox = match params.process.run {
            Run::Managed(run) => {
                let (proc_sender, proc_queue) = unbounded_channel();
                let env = ProcessEnv::new(
                    params.process.proc,
                    params.client,
                    params.options,
                    sender_ready,
                    Managed { queue: proc_queue },
                );

                tokio::spawn(async move {
                    client.report_process_terminated(id, run(env).await.err());
                });

                Mailbox::Tokio(proc_sender)
            }

            Run::Raw(run) => {
                let (proc_sender, proc_queue) = std::sync::mpsc::channel();
                let env = ProcessEnv::new(
                    params.process.proc,
                    params.client,
                    params.options,
                    sender_ready,
                    Raw {
                        queue: proc_queue,
                        handle: tokio::runtime::Handle::current(),
                    },
                );

                thread::spawn(move || {
                    client.report_process_terminated(id, run(env).err());
                });

                Mailbox::Raw(proc_sender)
            }
        };

        let process = RunningProc {
            id,
            proc: params.proc,
            mailbox,
            last_received_request: Uuid::nil(),
            dependents: Vec::new(),
        };
    });
}
