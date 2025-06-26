use std::{future::Future, sync::Arc, thread};

use tokio::sync::{mpsc::unbounded_channel, oneshot};
use uuid::Uuid;

use crate::{
    process::{manager::ManagerClient, Mailbox, Managed, ProcId, ProcessEnv, Raw, RunningProc},
    Options, Proc,
};

pub struct SpawnParams {
    pub id: ProcId,
    pub options: Arc<Options>,
    pub client: ManagerClient,
    pub process: Proc,
}

pub fn spawn_process(params: SpawnParams) {
    tokio::spawn(async move {
        let id = params.client.id();
        let client = params.client.clone();
        let (sender_ready, recv_ready) = oneshot::channel();
        let proc = params.process;

        let mailbox = match params.process {
            Proc::Root => return,
            Proc::Writing => todo!(),
            Proc::Reading => todo!(),
            Proc::Indexing => todo!(),
            Proc::PubSub => todo!(),
            Proc::Grpc => todo!(),
            Proc::PyroWorker => todo!(),
            #[cfg(test)]
            Proc::Echo => todo!(),
            #[cfg(test)]
            Proc::Sink => todo!(),
            #[cfg(test)]
            Proc::Panic => todo!(),
        };

        let process = RunningProc {
            id,
            proc,
            mailbox,
            last_received_request: Uuid::nil(),
            dependents: Vec::new(),
        };
    });
}

fn spawn_raw<F>(params: SpawnParams, sender_ready: oneshot::Sender<()>, run: F) -> Mailbox
where
    F: FnOnce(ProcessEnv<Raw>) -> eyre::Result<()> + Send + Sync + 'static,
{
    let (proc_sender, proc_queue) = std::sync::mpsc::channel();
    let env = ProcessEnv::new(
        params.process,
        params.client.clone(),
        params.options,
        sender_ready,
        Raw {
            queue: proc_queue,
            handle: tokio::runtime::Handle::current(),
        },
    );

    let id = params.id;
    let client = params.client;

    thread::spawn(move || {
        client.report_process_terminated(id, run(env).err());
    });

    Mailbox::Raw(proc_sender)
}

fn spawn<F, Fut>(params: SpawnParams, sender_ready: oneshot::Sender<()>, run: F) -> Mailbox
where
    F: FnOnce(ProcessEnv<Managed>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = eyre::Result<()>> + Send + 'static,
{
    let (proc_sender, proc_queue) = unbounded_channel();
    let env = ProcessEnv::new(
        params.process,
        params.client.clone(),
        params.options,
        sender_ready,
        Managed { queue: proc_queue },
    );

    let id = params.id;
    let client = params.client;

    tokio::spawn(async move {
        client.report_process_terminated(id, run(env).await.err());
    });

    Mailbox::Tokio(proc_sender)
}
