use bb8::Pool;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::chunks::ChunkContainer;
use geth_mikoshi::{FileSystemStorage, InMemoryStorage};
use messages::Messages;
use resource::{create_buffer_pool, BufferManager};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;
use std::{io, thread};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, Notify};
use uuid::Uuid;

use crate::Options;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod echo;
pub mod grpc;
pub mod indexing;
mod messages;
pub mod reading;
mod resource;
#[cfg(test)]
mod sink;
pub mod subscription;
// mod subscriptions;
pub mod writing;

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
    pub payload: Messages,
    pub created: Instant,
}

#[derive(Clone, Copy, PartialOrd, PartialEq, Eq, Ord, Hash, Debug)]
pub enum Proc {
    Writing,
    Reading,
    Indexing,
    PubSub,
    Grpc,
    #[cfg(test)]
    Echo,
    #[cfg(test)]
    Sink,
}

enum Topology {
    Singleton(Option<ProcId>),
    // Multiple(Vec<ProcId>),
}

struct RunningProc {
    id: ProcId,
    proc: Proc,
    mailbox: Mailbox,
}

pub struct Catalog {
    inner: HashMap<Proc, Topology>,
    monitor: HashMap<ProcId, RunningProc>,
}

impl Catalog {
    fn lookup(&self, proc: &Proc) -> eyre::Result<Option<ProcId>> {
        if let Some(topology) = self.inner.get(proc) {
            return match &topology {
                Topology::Singleton(prev) => Ok(*prev),
            };
        }

        eyre::bail!("process {:?} is not registered", proc);
    }

    fn report(&mut self, running: RunningProc) -> eyre::Result<()> {
        let proc = running.proc;
        if let Some(mut topology) = self.inner.get_mut(&running.proc) {
            match &mut topology {
                Topology::Singleton(prev) => {
                    if let Some(prev) = prev.as_ref() {
                        eyre::bail!(
                            "a {:?} process is already running on {:04}",
                            running.proc,
                            prev
                        );
                    }

                    *prev = Some(running.id);
                }
            }

            self.monitor.insert(running.id, running);
            return Ok(());
        }

        eyre::bail!("process {:?} is not registered in the catalog", proc);
    }

    fn terminate(&mut self, proc_id: ProcId) -> Option<RunningProc> {
        if let Some(running) = self.monitor.remove(&proc_id) {
            if let Some(mut topology) = self.inner.get_mut(&running.proc) {
                match &mut topology {
                    Topology::Singleton(prev) => {
                        *prev = None;
                    }
                }
            }

            return Some(running);
        }

        None
    }

    fn running_proc_len(&self) -> usize {
        self.monitor.len()
    }

    fn clear_running_processes(&mut self) {
        for proc_id in self.monitor.keys().copied().collect::<Vec<_>>() {
            self.terminate(proc_id);
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
    close_resp: Vec<oneshot::Sender<()>>,
    processes_shutting_down: usize,
}

impl<S> Manager<S>
where
    S: Storage + Send + Sync + 'static,
{
    fn gen_proc_id(&mut self) -> ProcId {
        let id = self.proc_id_gen;
        self.proc_id_gen += 1;
        id
    }

    fn handle(&mut self, cmd: ManagerCommand) -> eyre::Result<()> {
        if self.closing && !cmd.is_shutdown_related() {
            return Ok(());
        }

        match cmd {
            // ManagerCommand::Spawn { parent: _, resp: _ } => {
            //     // TODO - might change that command to start.
            // }
            ManagerCommand::Find { proc, resp } => {
                let _ = resp.send(self.catalog.lookup(&proc)?);
            }

            ManagerCommand::Send { dest, item, resp } => match item {
                Item::Mail(mail) => {
                    if let Some(resp) = self.requests.remove(&mail.correlation) {
                        let _ = resp.send(mail);
                    } else if let Some(proc) = self.catalog.monitor.get(&dest) {
                        if let Some(resp) = resp {
                            self.requests.insert(mail.correlation, resp);
                        }

                        let _ = proc.mailbox.send(Item::Mail(mail));
                    }
                }

                Item::Stream(stream) => {
                    if let Some(proc) = self.catalog.monitor.get(&dest) {
                        let _ = proc.mailbox.send(Item::Stream(stream));
                    }
                }
            },

            ManagerCommand::WaitFor { proc, resp } => {
                if let Some(proc_id) = self.catalog.lookup(&proc)? {
                    let _ = resp.send(proc_id);
                } else {
                    let id = self.gen_proc_id();
                    let runtime = self.runtime.clone();
                    let mut client = self.client.clone();
                    let options = self.options.clone();

                    client.id = id;

                    let running_proc = match proc {
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

                        #[cfg(test)]
                        Proc::Echo => spawn(options, client, proc, echo::run),

                        #[cfg(test)]
                        Proc::Sink => spawn(options, client, proc, sink::run),
                    };

                    let proc_id = running_proc.id;
                    self.catalog.report(running_proc)?;
                    let _ = resp.send(proc_id);
                }
            }

            ManagerCommand::ProcTerminated { id, error } => {
                if let Some(running) = self.catalog.terminate(id) {
                    if let Some(e) = error {
                        tracing::error!(
                            "process {:?}:{} terminated with error {}",
                            running.proc,
                            id,
                            e
                        );
                    } else {
                        tracing::info!("process {:?}:{} terminated", running.proc, id);
                    }
                }

                if self.closing {
                    self.processes_shutting_down = self
                        .processes_shutting_down
                        .checked_sub(1)
                        .unwrap_or_default();

                    if self.processes_shutting_down == 0 {
                        tracing::info!("process manager completed shutdown");
                        for resp in self.close_resp.drain(..) {
                            let _ = resp.send(());
                        }

                        self.queue.close();
                    }
                }
            }

            ManagerCommand::Shutdown { resp } => {
                if !self.closing {
                    self.closing = true;
                    self.processes_shutting_down = self.catalog.running_proc_len();

                    if self.processes_shutting_down == 0 {
                        let _ = resp.send(());
                        self.queue.close();
                        return Ok(());
                    }

                    self.catalog.clear_running_processes();
                }

                self.close_resp.push(resp);
            }
        }

        Ok(())
    }
}

pub struct Stream {
    correlation: Uuid,
    payload: Messages,
    sender: UnboundedSender<Messages>,
}

pub enum Item {
    Mail(Mail),
    Stream(Stream),
}

pub enum ManagerCommand {
    // Spawn {
    //     parent: Option<ProcId>,
    //     resp: oneshot::Sender<ProcId>,
    // },
    Find {
        proc: Proc,
        resp: oneshot::Sender<Option<ProcId>>,
    },

    Send {
        dest: ProcId,
        item: Item,
        resp: Option<oneshot::Sender<Mail>>,
    },

    WaitFor {
        proc: Proc,
        resp: oneshot::Sender<ProcId>,
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
    queue: std::sync::mpsc::Receiver<Item>,
    client: ManagerClient,
    handle: Handle,
}

#[derive(Clone)]
pub struct ManagerClient {
    id: ProcId,
    pool: Pool<BufferManager>,
    inner: UnboundedSender<ManagerCommand>,
    notify: Arc<Notify>,
}

impl ManagerClient {
    pub async fn find(&self, proc: Proc) -> eyre::Result<Option<ProcId>> {
        let (resp, receiver) = oneshot::channel();
        if self
            .inner
            .send(ManagerCommand::Find { proc, resp })
            .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        match receiver.await {
            Ok(id) => Ok(id),
            Err(_) => eyre::bail!("process manager has shutdown"),
        }
    }

    pub fn send(&self, dest: ProcId, payload: Messages) -> eyre::Result<()> {
        self.send_with_correlation(dest, Uuid::new_v4(), payload)
    }

    pub fn send_with_correlation(
        &self,
        dest: ProcId,
        correlation: Uuid,
        payload: Messages,
    ) -> eyre::Result<()> {
        if self
            .inner
            .send(ManagerCommand::Send {
                dest,
                item: Item::Mail(Mail {
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

    pub async fn request(&self, dest: ProcId, payload: Messages) -> eyre::Result<Mail> {
        let (resp, receiver) = oneshot::channel();
        if self
            .inner
            .send(ManagerCommand::Send {
                dest,
                item: Item::Mail(Mail {
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

        match receiver.await {
            Ok(mail) => Ok(mail),
            Err(_) => eyre::bail!("process manager has shutdown"),
        }
    }

    pub async fn request_stream(
        &self,
        dest: ProcId,
        payload: Messages,
    ) -> eyre::Result<UnboundedReceiver<Messages>> {
        let (sender, receiver) = unbounded_channel();
        if self
            .inner
            .send(ManagerCommand::Send {
                dest,
                item: Item::Stream(Stream {
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

    pub fn reply(&self, dest: ProcId, correlation: Uuid, payload: Messages) -> eyre::Result<()> {
        if self
            .inner
            .send(ManagerCommand::Send {
                dest,
                item: Item::Mail(Mail {
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

    pub async fn wait_for(&self, proc: Proc) -> eyre::Result<ProcId> {
        let (resp, receiver) = oneshot::channel();
        if self
            .inner
            .send(ManagerCommand::WaitFor { proc, resp })
            .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        tracing::debug!(
            target = format!("process-{}", self.id),
            "waiting for process {:?} to be available...",
            proc
        );
        match receiver.await {
            Ok(id) => {
                tracing::debug!(
                    target = format!("process-{}", self.id),
                    "resolved process {:?} to be {}",
                    proc,
                    id
                );

                Ok(id)
            }
            Err(_) => eyre::bail!("process manager has shutdown"),
        }
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
        .build();

    start_process_manager_with_catalog(options, catalog).await
}

pub async fn start_process_manager_with_catalog(
    options: Options,
    catalog: Catalog,
) -> eyre::Result<ManagerClient> {
    let notify = Arc::new(Notify::new());
    let pool = create_buffer_pool().await?;
    let (sender, queue) = unbounded_channel();
    let client = ManagerClient {
        id: 0,
        pool: pool.clone(),
        inner: sender.clone(),
        notify: notify.clone(),
    };

    let mgr_client = client.clone();
    let notify = mgr_client.notify.clone();

    if options.db == "in_mem" {
        let container = ChunkContainer::load(InMemoryStorage::new())?;
        tokio::spawn(async move {
            process_manager(options, mgr_client, catalog, container, queue).await;
            notify.notify_one();
        });
    } else {
        let container = load_fs_chunk_container(&options)?;
        tokio::spawn(async move {
            process_manager(options, mgr_client, catalog, container, queue).await;
            notify.notify_one();
        });
    }

    Ok(client)
}

fn load_fs_chunk_container(options: &Options) -> io::Result<ChunkContainer<FileSystemStorage>> {
    let storage = FileSystemStorage::new(options.db.clone().into())?;
    ChunkContainer::load(storage)
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
        close_resp: vec![],
        queue,
        processes_shutting_down: 0,
    };

    while let Some(cmd) = manager.queue.recv().await {
        if let Err(e) = manager.handle(cmd) {
            tracing::error!("unexpected: {}", e);
            break;
        }
    }
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
        queue: proc_queue,
        client: client.clone(),
        handle,
    };

    let sender = client.inner.clone();
    thread::spawn(move || {
        if let Err(e) = runnable(runtime, env) {
            tracing::error!("process {:?} terminated with error: {}", proc, e);
            let _ = sender.send(ManagerCommand::ProcTerminated { id, error: Some(e) });
        } else {
            tracing::info!("process {:?} terminated", proc);
            let _ = sender.send(ManagerCommand::ProcTerminated { id, error: None });
        }
    });

    RunningProc {
        id,
        proc,
        mailbox: Mailbox::Raw(proc_sender),
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
            tracing::error!("process {:?} terminated with error: {}", proc, e);
            let _ = sender.send(ManagerCommand::ProcTerminated { id, error: Some(e) });
        } else {
            tracing::info!("process {:?} terminated", proc);
            let _ = sender.send(ManagerCommand::ProcTerminated { id, error: None });
        }
    });

    RunningProc {
        id,
        proc,
        mailbox: Mailbox::Tokio(proc_sender),
    }
}
