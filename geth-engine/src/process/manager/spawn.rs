use std::{future::Future, sync::Arc, thread};

use tokio::sync::{mpsc::unbounded_channel, oneshot};
use uuid::Uuid;

#[cfg(test)]
use crate::process::{echo, panic, sink};
use crate::{
    process::{
        grpc, indexing,
        manager::ManagerClient,
        subscription::{self, pyro},
        writing, Mailbox, Managed, ProcId, ProcessEnv, Raw, RunningProc,
    },
    reading, Options, Proc,
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
            Proc::Writing => spawn_raw(params, sender_ready, writing::run),
            Proc::Reading => spawn_raw(params, sender_ready, reading::run),
            Proc::Indexing => spawn_raw(params, sender_ready, indexing::run),
            Proc::PubSub => spawn(params, sender_ready, subscription::run),
            Proc::Grpc => spawn(params, sender_ready, grpc::run),
            Proc::PyroWorker => spawn(params, sender_ready, pyro::worker::run),
            #[cfg(test)]
            Proc::Echo => spawn(params, sender_ready, echo::run),
            #[cfg(test)]
            Proc::Sink => spawn(params, sender_ready, sink::run),
            #[cfg(test)]
            Proc::Panic => spawn(params, sender_ready, panic::run),
        };

        let _ = recv_ready.await;
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
