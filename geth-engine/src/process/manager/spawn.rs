use geth_mikoshi::storage::Storage;
use tokio::runtime::Handle;

use crate::{
    process::{manager::ManagerClient, ProcessEnv, Raw, RunningProc, Runtime},
    Options, Proc,
};

pub struct Spawn {
    pub options: Options,
    pub client: ManagerClient,
    pub proc: Proc,
}

fn spawn_raw<S, F>(
    options: Options,
    client: ManagerClient,
    runtime: Runtime<S>,
    handle: Handle,
    proc: Proc,
    runnable: F,
) -> RunningProc
where
    S: Storage + Send + Sync + 'static,
    F: FnOnce(Runtime<S>, ProcessEnv<Raw>) -> eyre::Result<()> + Send + Sync + 'static,
{
    let id = client.id();
    let (proc_sender, proc_queue) = std::sync::mpsc::channel();
    let env = ProcessEnv::new(
        proc,
        client.clone(),
        options,
        Raw {
            queue: proc_queue,
            handle,
        },
    );

    let sender = client.inner.clone();
    thread::spawn(move || {
        if let Err(e) = runnable(runtime, env) {
            let error = e.to_string();
            if sender
                .send(ManagerCommand::ProcTerminated { id, error: Some(e) })
                .is_err()
            {
                tracing::error!(
                    proc_id = id,
                    ?proc,
                    error,
                    "cannot report process terminated with an error"
                );
            }
        } else if sender
            .send(ManagerCommand::ProcTerminated { id, error: None })
            .is_err()
        {
            tracing::error!(proc_id = id, ?proc, "cannot report process terminated");
        }
    });

    RunningProc {
        id,
        proc,
        mailbox: Mailbox::Raw(proc_sender),
        last_received_request: Uuid::nil(),
        dependents: Vec::new(),
    }
}

fn spawn<F, Fut>(options: Options, client: ManagerClient, proc: Proc, runnable: F) -> RunningProc
where
    F: FnOnce(ProcessEnv<Managed>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = eyre::Result<()>> + Send + 'static,
{
    let id = client.id;
    let (proc_sender, proc_queue) = unbounded_channel();
    let env = ProcessEnv::new(proc, client.clone(), options, Managed { queue: proc_queue });
    let sender = client.inner.clone();
    tokio::spawn(async move {
        if let Err(e) = runnable(env).await {
            let error = e.to_string();
            if sender
                .send(ManagerCommand::ProcTerminated { id, error: Some(e) })
                .is_err()
            {
                tracing::error!(
                    proc_id = id,
                    ?proc,
                    error,
                    "cannot report process terminated with an error"
                );
            }
        } else if sender
            .send(ManagerCommand::ProcTerminated { id, error: None })
            .is_err()
        {
            tracing::error!(proc_id = id, ?proc, "cannot report process terminated");
        }
    });

    RunningProc {
        id,
        proc,
        mailbox: Mailbox::Tokio(proc_sender),
        last_received_request: Uuid::nil(),
        dependents: Vec::new(),
    }
}
