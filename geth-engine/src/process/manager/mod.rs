use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use geth_common::ProgramSummary;
use geth_mikoshi::{wal::chunks::ChunkContainer, InMemoryStorage};
use tokio::{
    runtime::Handle,
    sync::{mpsc::UnboundedReceiver, oneshot, Notify},
};
use uuid::Uuid;

#[cfg(test)]
use crate::process::{echo, panic, sink};
use crate::{
    process::{
        grpc, indexing,
        manager::{
            catalog::ProvisionResult,
            proc::process_manager,
            spawn::{spawn_process, SpawnParams},
        },
        messages::{Messages, Notifications, Responses},
        subscription::{self, pyro},
        writing, Item, Mail, ProcId, SpawnError, SpawnResult,
    },
    reading, Options, Proc, RequestContext,
};

mod catalog;
mod client;
mod proc;
mod spawn;

pub use catalog::{Catalog, CatalogBuilder};
pub use client::ManagerClient;
pub use spawn::Process;

#[derive(Clone)]
pub struct ShutdownReporter {
    notify: Arc<Notify>,
    closed: Arc<AtomicBool>,
}

impl Default for ShutdownReporter {
    fn default() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl ShutdownReporter {
    pub fn report_shutdown(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::Release, Ordering::Acquire)
            .is_ok()
        {
            self.notify.notify_waiters();
        }
    }
}

#[derive(Clone, Debug)]
pub struct ShutdownNotification {
    notify: Arc<Notify>,
    closed: Arc<AtomicBool>,
}

impl From<ShutdownReporter> for ShutdownNotification {
    fn from(value: ShutdownReporter) -> Self {
        Self {
            notify: value.notify,
            closed: value.closed,
        }
    }
}

impl ShutdownNotification {
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

struct FindParams {
    proc: Proc,
    resp: oneshot::Sender<Option<ProgramSummary>>,
}

struct SendParams {
    dest: ProcId,
    item: Item,
    resp: Option<oneshot::Sender<Mail>>,
}

struct WaitForParams {
    origin: ProcId,
    proc: Proc,
    resp: oneshot::Sender<SpawnResult>,
}

struct ProcTerminatedParams {
    id: ProcId,
    error: Option<eyre::Report>,
}

struct ShutdownParams {
    resp: oneshot::Sender<()>,
}

struct TimeoutParams {
    correlation: Uuid,
}

pub enum ManagerCommand {
    Find(FindParams),
    Send(SendParams),
    WaitFor(WaitForParams),
    ProcTerminated(ProcTerminatedParams),
    Shutdown(ShutdownParams),
    Timeout(TimeoutParams),
}

impl ManagerCommand {
    fn is_shutdown_related(&self) -> bool {
        matches!(
            self,
            ManagerCommand::ProcTerminated { .. }
                | ManagerCommand::Shutdown { .. }
                | ManagerCommand::Timeout(_)
        )
    }
}

pub struct Manager {
    options: Options,
    client: ManagerClient,
    catalog: Catalog,
    proc_id_gen: ProcId,
    requests: HashMap<Uuid, oneshot::Sender<Mail>>,
    closing: bool,
    close_resp: Vec<oneshot::Sender<()>>,
    processes_shutting_down: HashMap<u64, Proc>,
    reporter: ShutdownReporter,
}

impl Manager {
    fn handle_find(&mut self, cmd: FindParams) -> eyre::Result<()> {
        let _ = cmd.resp.send(self.catalog.lookup(&cmd.proc)?);
    }

    fn handle_send(&mut self, cmd: SendParams) -> eyre::Result<()> {
        match cmd.item {
            Item::Mail(mail) => {
                if let Some(resp) = self.requests.remove(&mail.correlation) {
                    let _ = resp.send(mail);
                } else if let Some(proc) = self.catalog.get_process_mut(&cmd.dest) {
                    if let Some(resp) = cmd.resp {
                        self.requests.insert(mail.correlation, resp);
                        proc.last_received_request = mail.correlation;
                    } else {
                        proc.last_received_request = Uuid::nil();
                    }

                    if !proc.mailbox.send(Item::Mail(mail)) {
                        self.handle_terminate(ProcTerminatedParams {
                            id: cmd.dest,
                            error: None,
                        });
                    }
                }
            }

            Item::Stream(stream) => {
                if let Some(proc) = self.catalog.get_process(&cmd.dest) {
                    if !proc.mailbox.send(Item::Stream(stream)) {
                        self.handle_terminate(ProcTerminatedParams {
                            id: cmd.dest,
                            error: None,
                        });
                    }
                }
            }
        }
    }

    fn handle_wait_for(&mut self, cmd: WaitForParams) -> eyre::Result<()> {
        let provision = match self.catalog.provision_process(cmd.origin, cmd.proc)? {
            ProvisionResult::AlreadyProvisioned(id) => {
                let _ = cmd.resp.send(SpawnResult::Success(id));
                return Ok(());
            }

            ProvisionResult::LimitReached => {
                let _ = cmd.resp.send(SpawnResult::Failure {
                    proc: cmd.proc,
                    error: SpawnError::LimitReached,
                });

                return Ok(());
            }

            ProvisionResult::Available(p) => p,
        };

        let mut client = self
            .client
            .new_with_overrides(provision.id, provision.process.proc);

        spawn_process(SpawnParams {
            options: self.options.clone(),
            client,
            process: provision.process,
        });

        // // no need to track if the process that started this new process is the manager itself.
        // if cmd.origin != 0 {
        //     running_proc.dependents.push(cmd.origin);
        // }

        // self.catalog.monitor_process(running_proc);
        // let _ = cmd.resp.send(SpawnResult::Success(id));
    }

    fn handle_terminate(&mut self, cmd: ProcTerminatedParams) {
        if let Some(running) = self.catalog.remove_process(cmd.id) {
            if let Some(e) = cmd.error {
                tracing::error!(
                    error = %e,
                    id = cmd.id,
                    proc = ?running.proc,
                    closing = self.closing,
                    "process terminated with error",
                );
            } else {
                tracing::info!(id = cmd.id, proc = ?running.proc, closing = self.closing, "process terminated");
            }

            if let Some(resp) = self.requests.remove(&running.last_received_request) {
                tracing::warn!(
                    id = cmd.id,
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
                if let Some(running) = self.catalog.get_process(&dependent) {
                    if !self.closing
                        && !running.mailbox.send(Item::Mail(Mail {
                            context: RequestContext::new(),
                            origin: 0,
                            correlation: Uuid::nil(),
                            payload: Notifications::ProcessTerminated(cmd.id).into(),
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
                proc_id = cmd.id,
                "process is terminated but no runtime info were found"
            );
        }

        if self.closing {
            if let Some(proc) = self.processes_shutting_down.remove(&cmd.id) {
                tracing::info!(proc_id = cmd.id, ?proc, "process terminated");
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

    fn handle_shutdown(&mut self, cmd: ShutdownParams) -> eyre::Result<()> {
        tracing::info!("received shutdown request, initiating shutdown process");

        if !self.closing {
            self.closing = true;
            for proc in self.catalog.processes() {
                self.processes_shutting_down.insert(proc.id, proc.proc);
            }

            if self.processes_shutting_down.is_empty() {
                self.closed.store(true, Ordering::Release);
                let _ = cmd.resp.send(());
                self.queue.close();
                return Ok(());
            }

            tracing::debug!(
                running_procs = self.processes_shutting_down.len(),
                "shutdown process started"
            );

            self.catalog.clear_running_processes();
            let client = self.client.clone();

            // TODO - implement shutdown properly
            let corr = Uuid::new_v4();

            client.send_timeout_in(corr, Duration::from_secs(5));
        }

        self.close_resp.push(cmd.resp);
    }

    fn handle_timeout(&mut self, cmd: TimeoutParams) -> eyre::Result<()> {
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
            ManagerCommand::Timeout(cmd) => self.handle_timeout(cmd),
        }
    }
}

pub async fn start_process_manager_with_catalog(
    options: Options,
    catalog: Catalog,
) -> eyre::Result<ManagerClient> {
    let reporter = ShutdownReporter::default();
    let (client, queue) = ManagerClient::new_root_client(reporter.clone().into());
    let mgr_client = client.clone();

    if options.db == "in_mem" {
        let storage = InMemoryStorage::new();
        geth_mikoshi::storage::init(&storage)?;
        let container = ChunkContainer::load(storage)?;
        tokio::spawn(async move {
            process_manager(options, mgr_client, catalog, container, queue).await;
            reporter.report_shutdown();
        });
    } else {
        let container = load_fs_chunk_container(&options)?;
        tokio::spawn(async move {
            process_manager(options, mgr_client, catalog, container, queue).await;
            reporter.report_shutdown();
        });
    }

    Ok(client)
}
