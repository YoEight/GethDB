use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use geth_common::ProgramSummary;
use tokio::sync::{Notify, oneshot};
use uuid::Uuid;

use crate::{
    Options, Proc, RequestContext,
    process::{
        Item, Mail, ProcId, RunningProc, SpawnError, SpawnResult,
        manager::{
            catalog::ProvisionResult,
            proc::process_manager,
            spawn::{SpawnParams, spawn_process},
        },
        messages::{Messages, Notifications, Responses},
    },
};

mod catalog;
mod client;
mod proc;
mod spawn;

pub use catalog::{Catalog, CatalogBuilder};
pub use client::ManagerClient;

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

pub(crate) struct FindParams {
    proc: Proc,
    resp: oneshot::Sender<Option<ProgramSummary>>,
}

pub(crate) struct SendParams {
    dest: ProcId,
    item: Item,
    resp: Option<oneshot::Sender<Mail>>,
}

pub(crate) struct WaitForParams {
    origin: ProcId,
    proc: Proc,
    resp: oneshot::Sender<SpawnResult>,
}

pub(crate) struct ProcTerminatedParams {
    id: ProcId,
    error: Option<eyre::Report>,
}

pub(crate) struct ShutdownParams {
    resp: oneshot::Sender<()>,
}

pub(crate) enum TimeoutTarget {
    SpawnProcess(ProcId),
    Shutdown,
}

pub(crate) struct TimeoutParams {
    target: TimeoutTarget,
    correlation: Uuid,
}

pub(crate) struct ProcReadyParams {
    running: RunningProc,
    correlation: Uuid,
}

pub(crate) enum ManagerCommand {
    Find(FindParams),
    Send(SendParams),
    WaitFor(WaitForParams),
    ProcTerminated(ProcTerminatedParams),
    ProcReady(ProcReadyParams),
    Shutdown(ShutdownParams),
    Timeout(TimeoutParams),
}

pub struct Manager {
    options: Arc<Options>,
    client: ManagerClient,
    catalog: Catalog,
    requests: HashMap<Uuid, oneshot::Sender<Mail>>,
    closing: bool,
    close_resp: Vec<oneshot::Sender<()>>,
    processes_shutting_down: HashMap<u64, Proc>,
    reporter: ShutdownReporter,
}

impl Manager {
    fn handle_find(&mut self, cmd: FindParams) -> eyre::Result<()> {
        if self.closing {
            return Ok(());
        }

        let _ = cmd.resp.send(self.catalog.lookup(&cmd.proc)?);
        Ok(())
    }

    fn handle_send(&mut self, cmd: SendParams) -> eyre::Result<()> {
        if self.closing {
            return Ok(());
        }

        match cmd.item {
            Item::Mail(mail) => {
                if let Some(resp) = self.requests.remove(&mail.correlation) {
                    let _ = resp.send(mail);
                } else if let Some(proc) = self.catalog.get_process_mut(cmd.dest) {
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
                if let Some(proc) = self.catalog.get_process(cmd.dest) {
                    if !proc.mailbox.send(Item::Stream(stream)) {
                        self.handle_terminate(ProcTerminatedParams {
                            id: cmd.dest,
                            error: None,
                        });
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_wait_for(&mut self, cmd: WaitForParams) -> eyre::Result<()> {
        if self.closing {
            return Ok(());
        }

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

            ProvisionResult::WaitingForConfirmation(proc_id) => {
                self.catalog.wait_for_confirmation(proc_id, cmd.resp);
                return Ok(());
            }
        };

        let client = self.client.new_with_overrides(provision.id, provision.proc);
        let correlation = spawn_process(SpawnParams {
            options: self.options.clone(),
            client,
            process: provision.proc,
            id: provision.id,
        });

        self.catalog
            .create_waiting_room(correlation, provision.id, provision.proc, cmd.resp)
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
                if let Some(running) = self.catalog.get_process(dependent) {
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
                self.reporter.report_shutdown();

                for resp in self.close_resp.drain(..) {
                    let _ = resp.send(());
                }
            }
        }
    }

    fn handle_shutdown(&mut self, cmd: ShutdownParams) -> eyre::Result<()> {
        if !self.closing {
            tracing::info!("received shutdown request, initiating shutdown process");

            self.closing = true;
            for proc in self.catalog.processes() {
                self.processes_shutting_down.insert(proc.id, proc.proc);
            }

            if self.processes_shutting_down.is_empty() {
                self.reporter.report_shutdown();
                let _ = cmd.resp.send(());
                return Ok(());
            }

            tracing::debug!(
                running_procs = self.processes_shutting_down.len(),
                "shutdown process started"
            );

            self.catalog.clear_running_processes();
            self.client.send_timeout_in(
                Uuid::nil(),
                TimeoutTarget::Shutdown,
                Duration::from_secs(5),
            );
        }

        self.close_resp.push(cmd.resp);

        Ok(())
    }

    fn handle_proc_ready(&mut self, cmd: ProcReadyParams) {
        self.catalog
            .report_process_ready(cmd.correlation, cmd.running);
    }

    fn handle_timeout(&mut self, cmd: TimeoutParams) {
        match cmd.target {
            TimeoutTarget::SpawnProcess(id) => {
                self.catalog
                    .report_process_start_timeout(id, cmd.correlation);
            }

            TimeoutTarget::Shutdown => {
                tracing::warn!("shutdown process timed out");

                for (id, proc) in self.processes_shutting_down.iter() {
                    tracing::warn!(proc_id = id, ?proc, "process didn't terminate in time");
                }

                self.reporter.report_shutdown();
                for resp in self.close_resp.drain(..) {
                    let _ = resp.send(());
                }
            }
        }
    }
}

pub async fn start_process_manager_with_catalog(
    options: Options,
    catalog: Catalog,
) -> eyre::Result<ManagerClient> {
    let reporter = ShutdownReporter::default();
    let (client, queue) = ManagerClient::new_root_client(reporter.clone().into());

    process_manager(options, client.clone(), catalog, reporter, queue);

    Ok(client)
}
