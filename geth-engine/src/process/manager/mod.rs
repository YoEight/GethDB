use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use geth_common::ProgramSummary;
use geth_mikoshi::{storage::Storage, wal::chunks::ChunkContainer, InMemoryStorage};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver},
    oneshot, Notify,
};
use uuid::Uuid;

use crate::{
    process::{
        load_fs_chunk_container, manager::proc::process_manager, Item, Mail, ProcId, Runtime,
        SpawnError, SpawnResult, Topology,
    },
    Options, Proc,
};

mod catalog;
mod client;
mod proc;

pub use catalog::{Catalog, CatalogBuilder};
pub use client::ManagerClient;

#[derive(Clone)]
pub struct ShutdownNotification {
    notify: Arc<Notify>,
    closed: Arc<AtomicBool>,
}

impl ShutdownNotification {
    pub fn new() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub async fn wait_for_shutdown(self) {
        if self.is_shutdown() {
            return;
        }

        self.notify.notified().await
    }
}

struct Find {
    proc: Proc,
    resp: oneshot::Sender<Option<ProgramSummary>>,
}

struct Send {
    dest: ProcId,
    item: Item,
    resp: Option<oneshot::Sender<Mail>>,
}

struct WaitFor {
    origin: ProcId,
    proc: Proc,
    resp: oneshot::Sender<SpawnResult>,
}

struct ProcTerminated {
    id: ProcId,
    error: Option<eyre::Report>,
}

struct Shutdown {
    resp: oneshot::Sender<()>,
}

pub enum ManagerCommand {
    Find(Find),
    Send(Send),
    WaitFor(WaitFor),
    ProcTerminated(ProcTerminated),
    Shutdown(Shutdown),
    ShutdownTimeout,
}

impl ManagerCommand {
    fn is_shutdown_related(&self) -> bool {
        matches!(
            self,
            ManagerCommand::ProcTerminated { .. }
                | ManagerCommand::Shutdown { .. }
                | ManagerCommand::ShutdownTimeout
        )
    }
}

pub struct Manager<S> {
    options: Options,
    client: ManagerClient,
    runtime: Runtime<S>,
    catalog: Catalog,
    proc_id_gen: ProcId,
    requests: HashMap<Uuid, oneshot::Sender<Mail>>,
    queue: UnboundedReceiver<ManagerCommand>,
    closing: bool,
    closed: Arc<AtomicBool>,
    close_resp: Vec<oneshot::Sender<()>>,
    processes_shutting_down: HashMap<u64, Proc>,
}

impl<S> Manager<S>
where
    S: Storage + Send + Sync + 'static,
{
    fn handle_find(&mut self, cmd: Find) -> eyre::Result<()> {
        let _ = cmd.resp.send(self.catalog.lookup(&proc)?);
    }

    fn handle_send(&mut self, cmd: Send) -> eyre::Result<()> {
        match cmd.item {
            Item::Mail(mail) => {
                if let Some(resp) = self.requests.remove(&mail.correlation) {
                    let _ = resp.send(mail);
                } else if let Some(proc) = self.catalog.monitor.get_mut(&cmd.dest) {
                    if let Some(resp) = cmd.resp {
                        self.requests.insert(mail.correlation, resp);
                        proc.last_received_request = mail.correlation;
                    } else {
                        proc.last_received_request = Uuid::nil();
                    }

                    if !proc.mailbox.send(Item::Mail(mail)) {
                        self.handle_terminate(cmd.dest, None);
                    }
                }
            }

            Item::Stream(stream) => {
                if let Some(proc) = self.catalog.monitor.get(&cmd.dest) {
                    if !proc.mailbox.send(Item::Stream(stream)) {
                        self.handle_terminate(cmd.dest, None);
                    }
                }
            }
        }
    }

    fn handle_wait_for(&mut self, cmd: WaitFor) -> eyre::Result<()> {
        let id = if let Some(topology) = self.catalog.inner.get_mut(&proc) {
            match topology {
                Topology::Singleton(prev) => {
                    if let Some(id) = prev.as_ref() {
                        // no need to track if the process that started this new process is the manager itself.
                        if origin != 0 {
                            if let Some(running) = self.catalog.monitor.get_mut(id) {
                                running.dependents.push(origin);
                            } else {
                                tracing::error!(id = id, proc = ?proc, "running process was expected but is not found");
                                panic!();
                            }
                        }

                        let _ = resp.send(SpawnResult::Success(*id));
                        return Ok(());
                    } else {
                        let id = self.proc_id_gen;
                        self.proc_id_gen += 1;
                        *prev = Some(id);
                        id
                    }
                }

                Topology::Multiple { limit, instances } => {
                    if instances.len() + 1 > *limit {
                        let _ = resp.send(SpawnResult::Failure {
                            proc,
                            error: SpawnError::LimitReached,
                        });
                        return Ok(());
                    }

                    let id = self.proc_id_gen;
                    self.proc_id_gen += 1;

                    instances.insert(id);
                    id
                }
            }
        } else {
            eyre::bail!("process {:?} is not registered", proc);
        };

        let runtime = self.runtime.clone();
        let mut client = self.client.new_with_overrides(id, proc);
        let options = self.options.clone();

        let mut running_proc = match proc {
            Proc::Writing => spawn_raw(
                options,
                client,
                runtime,
                Handle::current(),
                proc,
                writing::run,
            ),

            Proc::Reading => spawn_raw(
                options,
                client,
                runtime,
                Handle::current(),
                proc,
                reading::run,
            ),

            Proc::Indexing => spawn_raw(
                options,
                client,
                runtime,
                Handle::current(),
                proc,
                indexing::run,
            ),

            Proc::PubSub => spawn(options, client, proc, subscription::run),
            Proc::Grpc => spawn(options, client, proc, grpc::run),

            Proc::PyroWorker => spawn(options, client, proc, pyro::worker::run),

            Proc::Root => {
                let _ = resp.send(SpawnResult::Success(0));
                return Ok(());
            }

            #[cfg(test)]
            Proc::Echo => spawn(options, client, proc, echo::run),

            #[cfg(test)]
            Proc::Sink => spawn(options, client, proc, sink::run),

            #[cfg(test)]
            Proc::Panic => spawn(options, client, proc, panic::run),
        };

        // no need to track if the process that started this new process is the manager itself.
        if origin != 0 {
            running_proc.dependents.push(origin);
        }

        self.catalog.monitor.insert(id, running_proc);
        let _ = resp.send(SpawnResult::Success(id));
    }

    fn handle_terminate(&mut self, cmd: ProcTerminated) {
        if let Some(running) = self.catalog.monitor.remove(&id) {
            self.catalog.remove(&running);

            if let Some(e) = error {
                tracing::error!(
                    error = %e,
                    id = id,
                    proc = ?running.proc,
                    closing = self.closing,
                    "process terminated with error",
                );
            } else {
                tracing::info!(id = id, proc = ?running.proc, closing = self.closing, "process terminated");
            }

            if let Some(resp) = self.requests.remove(&running.last_received_request) {
                tracing::warn!(
                    id = id,
                    proc = ?running.proc,
                    closing = self.closing,
                    "process terminated with pending request",
                );

                let _ = resp.send(Mail {
                    context: RequestContext::new(),
                    origin: running.id,
                    correlation: running.last_received_request,
                    payload: Messages::Responses(Responses::FatalError),
                    created: Instant::now(),
                });
            }

            for dependent in running.dependents {
                if let Some(running) = self.catalog.monitor.get(&dependent) {
                    if !self.closing
                        && !running.mailbox.send(Item::Mail(Mail {
                            context: RequestContext::new(),
                            origin: 0,
                            correlation: Uuid::nil(),
                            payload: Notifications::ProcessTerminated(id).into(),
                            created: Instant::now(),
                        }))
                    {
                        // I don't want to call `handle_terminate` here because it could end up blowing up the stack.
                        // I could rewrite `handle_terminate` to avoid recursion.
                        tracing::warn!(id = dependent, proc = ?running.proc, closing = self.closing, "process seems to be terminated");
                    }
                }
            }
        } else if !self.closing {
            tracing::warn!(
                proc_id = id,
                "process is terminated but no runtime info were found"
            );
        }

        if self.closing {
            if let Some(proc) = self.processes_shutting_down.remove(&id) {
                tracing::info!(proc_id = id, ?proc, "process terminated");
            }

            if self.processes_shutting_down.is_empty() {
                self.closed.store(true, Ordering::Release);
                for resp in self.close_resp.drain(..) {
                    let _ = resp.send(());
                }

                self.queue.close();
            }
        }
    }

    fn handle_shutdown(&mut self, cmd: Shutdown) -> eyre::Result<()> {
        tracing::info!("received shutdown request, initiating shutdown process");

        if !self.closing {
            self.closing = true;
            for (pid, proc) in self.catalog.monitor.iter() {
                self.processes_shutting_down.insert(*pid, proc.proc);
            }

            if self.processes_shutting_down.is_empty() {
                self.closed.store(true, Ordering::Release);
                let _ = resp.send(());
                self.queue.close();
                return Ok(());
            }

            tracing::debug!(
                running_procs = self.processes_shutting_down.len(),
                "shutdown process started"
            );

            self.catalog.clear_running_processes();
            let client = self.client.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let _ = client.inner.send(ManagerCommand::ShutdownTimeout);
            });
        }

        self.close_resp.push(resp);
    }

    fn handle_shutdown_timeout(&mut self) -> eyre::Result<()> {
        tracing::warn!("shutdown process timedout");

        for (id, proc) in self.processes_shutting_down.iter() {
            tracing::warn!(proc_id = id, ?proc, "process didn't terminate in time");
        }

        self.closed.store(true, Ordering::Release);
        for resp in self.close_resp.drain(..) {
            let _ = resp.send(());
        }
        self.queue.close();
        Ok(())
    }

    fn handle(&mut self, cmd: ManagerCommand) -> eyre::Result<()> {
        if self.closing && !cmd.is_shutdown_related() {
            return Ok(());
        }

        match cmd {
            ManagerCommand::Find(cmd) => self.handle_find(cmd),
            ManagerCommand::Send(cmd) => self.handle_send(cmd),
            ManagerCommand::WaitFor(cmd) => self.handle_wait_for(cmd),
            ManagerCommand::ProcTerminated(cmd) => self.handle_terminate(cmd),
            ManagerCommand::Shutdown(cmd) => self.handle_shutdown(cmd),
            ManagerCommand::ShutdownTimeout => self.handle_shutdown_timeout(),
        }
    }
}

pub async fn start_process_manager_with_catalog(
    options: Options,
    catalog: Catalog,
) -> eyre::Result<ManagerClient> {
    let (client, queue) = ManagerClient::new_root_client();

    let mgr_client = client.clone();
    let notify = mgr_client.notify.clone();

    if options.db == "in_mem" {
        let storage = InMemoryStorage::new();
        geth_mikoshi::storage::init(&storage)?;
        let container = ChunkContainer::load(storage)?;
        tokio::spawn(async move {
            process_manager(options, mgr_client, catalog, container, queue).await;
            closed.store(true, Ordering::Release);
            notify.notify_waiters();
        });
    } else {
        let container = load_fs_chunk_container(&options)?;
        tokio::spawn(async move {
            process_manager(options, mgr_client, catalog, container, queue).await;
            closed.store(true, Ordering::Release);
            notify.notify_waiters();
        });
    }

    Ok(client)
}
