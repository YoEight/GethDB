use chrono::Utc;
use geth_common::ProgramSummary;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::chunks::ChunkContainer;
use geth_mikoshi::{FileSystemStorage, InMemoryStorage};
use messages::{Messages, Responses};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use subscription::{pyro, SubscriptionClient};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, Notify};
use tracing::instrument;
use uuid::Uuid;

use crate::process::messages::Notifications;
use crate::{IndexClient, Options, ReaderClient, WriterClient};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod echo;
pub mod grpc;
pub mod indexing;
mod messages;
#[cfg(test)]
mod panic;
pub mod reading;
#[cfg(test)]
mod sink;
pub mod subscription;
// mod subscriptions;
pub mod writing;

#[derive(Debug, Clone, Copy)]
pub struct RequestContext {
    pub correlation: Uuid,
}

impl RequestContext {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        RequestContext {
            correlation: Uuid::new_v4(),
        }
    }
}

#[derive(Clone)]
enum Mailbox {
    Tokio(UnboundedSender<Item>),
    Raw(std::sync::mpsc::Sender<Item>),
}

impl Mailbox {
    pub fn send(&self, item: Item) -> bool {
        match self {
            Mailbox::Tokio(x) => x.send(item).is_ok(),
            Mailbox::Raw(x) => x.send(item).is_ok(),
        }
    }
}

pub type ProcId = u64;

pub struct Mail {
    pub origin: ProcId,
    pub correlation: Uuid,
    pub context: RequestContext,
    pub payload: Messages,
    pub created: Instant,
}

#[derive(Clone, Copy, PartialOrd, PartialEq, Eq, Ord, Hash, Debug)]
pub enum Proc {
    Root,
    Writing,
    Reading,
    Indexing,
    PubSub,
    Grpc,
    PyroWorker,
    #[cfg(test)]
    Echo,
    #[cfg(test)]
    Sink,
    #[cfg(test)]
    Panic,
}

enum Topology {
    Singleton(Option<ProcId>),
    Multiple {
        limit: usize,
        instances: HashSet<ProcId>,
    },
}

struct RunningProc {
    id: ProcId,
    proc: Proc,
    last_received_request: Uuid,
    mailbox: Mailbox,
    dependents: Vec<ProcId>,
}

pub struct Catalog {
    inner: HashMap<Proc, Topology>,
    monitor: HashMap<ProcId, RunningProc>,
}

impl Catalog {
    fn lookup(&self, proc: &Proc) -> eyre::Result<Option<ProgramSummary>> {
        if let Some(topology) = self.inner.get(proc) {
            return match &topology {
                Topology::Singleton(prev) => {
                    if let Some(run) = prev.as_ref().and_then(|p| self.monitor.get(p)) {
                        Ok(Some(ProgramSummary {
                            id: run.id,
                            name: format!("{:?}", proc),
                            started_at: Utc::now(), // TODO - no use for date times.
                        }))
                    } else {
                        Ok(None)
                    }
                }

                Topology::Multiple { .. } => {
                    // It doesn't really make sense for this topology.
                    Ok(None)
                }
            };
        }

        eyre::bail!("process {:?} is not registered", proc);
    }

    fn remove(&mut self, running: &RunningProc) {
        if let Some(mut topology) = self.inner.get_mut(&running.proc) {
            match &mut topology {
                Topology::Singleton(prev) => {
                    *prev = None;
                }

                Topology::Multiple { instances, .. } => {
                    instances.remove(&running.id);
                }
            }
        }
    }

    fn clear_running_processes(&mut self) {
        for (_, running) in std::mem::take(&mut self.monitor) {
            self.remove(&running);
            tracing::debug!(proc_id = running.id, proc = ?running.proc, "process cleared from the monitor and the catalog");
        }
    }
}

impl Catalog {
    pub fn builder() -> CatalogBuilder {
        CatalogBuilder {
            inner: HashMap::new(),
        }
    }
}

pub struct CatalogBuilder {
    inner: HashMap<Proc, Topology>,
}

impl CatalogBuilder {
    pub fn register(mut self, proc: Proc) -> Self {
        self.inner.insert(proc, Topology::Singleton(None));

        self
    }

    pub fn register_multiple(mut self, proc: Proc, limit: usize) -> Self {
        self.inner.insert(
            proc,
            Topology::Multiple {
                limit,
                instances: Default::default(),
            },
        );

        self
    }

    pub fn build(self) -> Catalog {
        Catalog {
            inner: self.inner,
            monitor: Default::default(),
        }
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
    closed: bool,
    close_resp: Vec<oneshot::Sender<()>>,
    processes_shutting_down: HashMap<u64, Proc>,
}

impl<S> Manager<S>
where
    S: Storage + Send + Sync + 'static,
{
    fn handle(&mut self, cmd: ManagerCommand) -> eyre::Result<()> {
        if self.closing && !cmd.is_shutdown_related() {
            return Ok(());
        }

        match cmd {
            ManagerCommand::Find { proc, resp } => {
                let _ = resp.send(self.catalog.lookup(&proc)?);
            }

            ManagerCommand::Send { dest, item, resp } => match item {
                Item::Mail(mail) => {
                    if let Some(resp) = self.requests.remove(&mail.correlation) {
                        let _ = resp.send(mail);
                    } else if let Some(proc) = self.catalog.monitor.get_mut(&dest) {
                        if let Some(resp) = resp {
                            self.requests.insert(mail.correlation, resp);
                            proc.last_received_request = mail.correlation;
                        } else {
                            proc.last_received_request = Uuid::nil();
                        }

                        if !proc.mailbox.send(Item::Mail(mail)) {
                            self.handle_terminate(dest, None);
                        }
                    }
                }

                Item::Stream(stream) => {
                    if let Some(proc) = self.catalog.monitor.get(&dest) {
                        if !proc.mailbox.send(Item::Stream(stream)) {
                            self.handle_terminate(dest, None);
                        }
                    }
                }
            },

            ManagerCommand::WaitFor { origin, proc, resp } => {
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
                let mut client = self.client.clone();
                let options = self.options.clone();

                client.id = id;
                client.origin_proc = proc;

                let mut running_proc = match proc {
                    Proc::Writing => {
                        spawn_raw(client, runtime, Handle::current(), proc, writing::run)
                    }

                    Proc::Reading => {
                        spawn_raw(client, runtime, Handle::current(), proc, reading::run)
                    }

                    Proc::Indexing => {
                        spawn_raw(client, runtime, Handle::current(), proc, indexing::run)
                    }

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

            ManagerCommand::ProcTerminated { id, error } => self.handle_terminate(id, error),

            ManagerCommand::Shutdown { resp } => {
                tracing::info!("received shutdown request, initiating shutdown process");

                if !self.closing {
                    self.closing = true;
                    for (pid, proc) in self.catalog.monitor.iter() {
                        self.processes_shutting_down.insert(*pid, proc.proc);
                    }

                    if self.processes_shutting_down.is_empty() {
                        let _ = resp.send(());
                        self.queue.close();
                        self.closed = true;
                        return Ok(());
                    }

                    tracing::debug!(
                        running_procs = self.processes_shutting_down.len(),
                        "shutdown process started"
                    );

                    self.catalog.clear_running_processes();
                }

                self.close_resp.push(resp);
            }
        }

        Ok(())
    }

    fn handle_terminate(&mut self, id: ProcId, error: Option<eyre::Report>) {
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
                for resp in self.close_resp.drain(..) {
                    let _ = resp.send(());
                }

                self.queue.close();
                self.closed = true;
            }
        }
    }
}

pub struct Stream {
    context: RequestContext,
    correlation: Uuid,
    payload: Messages,
    sender: UnboundedSender<Messages>,
}

pub enum Item {
    Mail(Mail),
    Stream(Stream),
}

pub enum SpawnResult {
    Success(ProcId),
    Failure { proc: Proc, error: SpawnError },
}

impl SpawnResult {
    pub fn ok(&self) -> Option<ProcId> {
        match self {
            SpawnResult::Success(id) => Some(*id),
            _ => None,
        }
    }

    pub fn must_succeed(self) -> eyre::Result<ProcId> {
        match self {
            SpawnResult::Success(id) => Ok(id),
            SpawnResult::Failure { proc, error } => match error {
                SpawnError::LimitReached => eyre::bail!("process {:?} limit reached", proc),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpawnError {
    LimitReached,
}

pub enum ManagerCommand {
    Find {
        proc: Proc,
        resp: oneshot::Sender<Option<ProgramSummary>>,
    },

    Send {
        dest: ProcId,
        item: Item,
        resp: Option<oneshot::Sender<Mail>>,
    },

    WaitFor {
        origin: ProcId,
        proc: Proc,
        resp: oneshot::Sender<SpawnResult>,
    },

    ProcTerminated {
        id: ProcId,
        error: Option<eyre::Report>,
    },

    Shutdown {
        resp: oneshot::Sender<()>,
    },
}

impl ManagerCommand {
    fn is_shutdown_related(&self) -> bool {
        matches!(
            self,
            ManagerCommand::ProcTerminated { .. } | ManagerCommand::Shutdown { .. }
        )
    }
}

pub struct ProcessEnv {
    queue: UnboundedReceiver<Item>,
    client: ManagerClient,
    options: Options,
}

pub struct ProcessRawEnv {
    proc: Proc,
    queue: std::sync::mpsc::Receiver<Item>,
    client: ManagerClient,
    handle: Handle,
}

#[derive(Clone, Debug)]
pub struct ManagerClient {
    id: ProcId,
    origin_proc: Proc,
    inner: UnboundedSender<ManagerCommand>,
    notify: Arc<Notify>,
}

impl ManagerClient {
    pub async fn find(&self, proc: Proc) -> eyre::Result<Option<ProgramSummary>> {
        let (resp, receiver) = oneshot::channel();
        if self
            .inner
            .send(ManagerCommand::Find { proc, resp })
            .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        match receiver.await {
            Ok(ps) => Ok(ps),
            Err(_) => eyre::bail!("process manager has shutdown"),
        }
    }

    pub fn send(
        &self,
        context: RequestContext,
        dest: ProcId,
        payload: Messages,
    ) -> eyre::Result<()> {
        self.send_with_correlation(context, dest, Uuid::new_v4(), payload)
    }

    pub fn send_with_correlation(
        &self,
        context: RequestContext,
        dest: ProcId,
        correlation: Uuid,
        payload: Messages,
    ) -> eyre::Result<()> {
        if self
            .inner
            .send(ManagerCommand::Send {
                dest,
                item: Item::Mail(Mail {
                    context,
                    origin: self.id,
                    correlation,
                    payload,
                    created: Instant::now(),
                }),
                resp: None,
            })
            .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        Ok(())
    }

    pub async fn request_opt(
        &self,
        context: RequestContext,
        dest: ProcId,
        payload: Messages,
    ) -> eyre::Result<Option<Mail>> {
        let (resp, receiver) = oneshot::channel();
        if self
            .inner
            .send(ManagerCommand::Send {
                dest,
                item: Item::Mail(Mail {
                    context,
                    origin: self.id,
                    correlation: Uuid::new_v4(),
                    payload,
                    created: Instant::now(),
                }),
                resp: Some(resp),
            })
            .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        Ok(receiver.await.ok())
    }

    pub async fn request(
        &self,
        context: RequestContext,
        dest: ProcId,
        payload: Messages,
    ) -> eyre::Result<Mail> {
        if let Some(mail) = self.request_opt(context, dest, payload).await? {
            Ok(mail)
        } else {
            eyre::bail!("process manager has shutdown")
        }
    }

    pub async fn request_stream(
        &self,
        context: RequestContext,
        dest: ProcId,
        payload: Messages,
    ) -> eyre::Result<UnboundedReceiver<Messages>> {
        let (sender, receiver) = unbounded_channel();
        if self
            .inner
            .send(ManagerCommand::Send {
                dest,
                item: Item::Stream(Stream {
                    context,
                    correlation: Uuid::new_v4(),
                    payload,
                    sender,
                }),
                resp: None,
            })
            .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        Ok(receiver)
    }

    pub fn reply(
        &self,
        context: RequestContext,
        dest: ProcId,
        correlation: Uuid,
        payload: Messages,
    ) -> eyre::Result<()> {
        if self
            .inner
            .send(ManagerCommand::Send {
                dest,
                item: Item::Mail(Mail {
                    context,
                    origin: self.id,
                    correlation,
                    payload,
                    created: Instant::now(),
                }),
                resp: None,
            })
            .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        Ok(())
    }

    #[instrument(skip(self), fields(origin = ?self.origin_proc))]
    pub async fn wait_for(&self, proc: Proc) -> eyre::Result<SpawnResult> {
        let (resp, receiver) = oneshot::channel();
        if self
            .inner
            .send(ManagerCommand::WaitFor {
                origin: self.id,
                proc,
                resp,
            })
            .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        tracing::debug!(proc = ?proc, "waiting for process to be available...");

        match receiver.await {
            Ok(res) => {
                if let Some(id) = res.ok() {
                    tracing::info!(proc = ?proc, id = %id, "process resolved");
                }

                Ok(res)
            }
            Err(_) => eyre::bail!("process manager has shutdown"),
        }
    }

    pub async fn new_writer_client(&self) -> eyre::Result<WriterClient> {
        let id = self.wait_for(Proc::Writing).await?.must_succeed()?;
        Ok(WriterClient::new(id, self.clone()))
    }

    pub async fn new_subscription_client(&self) -> eyre::Result<SubscriptionClient> {
        let id = self.wait_for(Proc::PubSub).await?.must_succeed()?;
        Ok(SubscriptionClient::new(id, self.clone()))
    }

    pub async fn new_index_client(&self) -> eyre::Result<IndexClient> {
        let id = self.wait_for(Proc::Indexing).await?.must_succeed()?;
        Ok(IndexClient::new(id, self.clone()))
    }

    pub async fn new_reader_client(&self) -> eyre::Result<ReaderClient> {
        let id = self.wait_for(Proc::Reading).await?.must_succeed()?;
        Ok(ReaderClient::new(id, self.clone()))
    }

    pub async fn shutdown(&self) -> eyre::Result<()> {
        let (resp, recv) = oneshot::channel();
        if self.inner.send(ManagerCommand::Shutdown { resp }).is_err() {
            return Ok(());
        }

        let _ = recv.await;
        Ok(())
    }

    pub async fn manager_exited(self) {
        self.notify.notified().await;
    }
}

pub async fn start_process_manager(options: Options) -> eyre::Result<ManagerClient> {
    let catalog = Catalog::builder()
        .register(Proc::Indexing)
        .register(Proc::Writing)
        .register(Proc::Reading)
        .register(Proc::PubSub)
        .register(Proc::Grpc)
        .register_multiple(Proc::PyroWorker, 8)
        .build();

    start_process_manager_with_catalog(options, catalog).await
}

pub async fn start_process_manager_with_catalog(
    options: Options,
    catalog: Catalog,
) -> eyre::Result<ManagerClient> {
    let notify = Arc::new(Notify::new());
    let (sender, queue) = unbounded_channel();
    let client = ManagerClient {
        id: 0,
        origin_proc: Proc::Root,
        inner: sender.clone(),
        notify: notify.clone(),
    };

    let mgr_client = client.clone();
    let notify = mgr_client.notify.clone();

    if options.db == "in_mem" {
        let storage = InMemoryStorage::new();
        geth_mikoshi::storage::init(&storage)?;
        let container = ChunkContainer::load(storage)?;
        tokio::spawn(async move {
            process_manager(options, mgr_client, catalog, container, queue).await;
            notify.notify_waiters();
        });
    } else {
        let container = load_fs_chunk_container(&options)?;
        tokio::spawn(async move {
            process_manager(options, mgr_client, catalog, container, queue).await;
            notify.notify_waiters();
        });
    }

    Ok(client)
}

fn load_fs_chunk_container(options: &Options) -> eyre::Result<ChunkContainer<FileSystemStorage>> {
    let storage = FileSystemStorage::new(options.db.clone().into())?;
    geth_mikoshi::storage::init(&storage)?;

    let container = ChunkContainer::load(storage)?;
    Ok(container)
}

#[derive(Clone)]
pub struct Runtime<S> {
    container: ChunkContainer<S>,
}

impl<S> Runtime<S> {
    pub fn container(&self) -> &ChunkContainer<S> {
        &self.container
    }
}

async fn process_manager<S>(
    options: Options,
    client: ManagerClient,
    catalog: Catalog,
    container: ChunkContainer<S>,
    queue: UnboundedReceiver<ManagerCommand>,
) where
    S: Storage + Send + Sync + 'static,
{
    let mut manager = Manager {
        options,
        client,
        runtime: Runtime { container },
        catalog,
        proc_id_gen: 1,
        requests: Default::default(),
        closing: false,
        closed: false,
        close_resp: vec![],
        queue,
        processes_shutting_down: Default::default(),
    };

    while let Some(cmd) = manager.queue.recv().await {
        if let Err(e) = manager.handle(cmd) {
            tracing::error!("unexpected: {}", e);
            break;
        }

        if manager.closed {
            break;
        }
    }

    tracing::info!("process manager terminated");
}

fn spawn_raw<S, F>(
    client: ManagerClient,
    runtime: Runtime<S>,
    handle: Handle,
    proc: Proc,
    runnable: F,
) -> RunningProc
where
    S: Storage + Send + Sync + 'static,
    F: FnOnce(Runtime<S>, ProcessRawEnv) -> eyre::Result<()> + Send + Sync + 'static,
{
    let id = client.id;
    let (proc_sender, proc_queue) = std::sync::mpsc::channel();
    let env = ProcessRawEnv {
        proc,
        queue: proc_queue,
        client: client.clone(),
        handle,
    };

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
    F: FnOnce(ProcessEnv) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = eyre::Result<()>> + Send + 'static,
{
    let id = client.id;
    let (proc_sender, proc_queue) = unbounded_channel();
    let env = ProcessEnv {
        queue: proc_queue,
        client: client.clone(),
        options,
    };

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
