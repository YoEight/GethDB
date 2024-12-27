use bytes::{Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;
use std::{io, thread};
use strum_macros::EnumString;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::bus::{
    GetProgrammableSubscriptionStatsMsg, KillProgrammableSubscriptionMsg, SubscribeMsg,
};
use crate::domain::index::IndexRef;
use crate::messages::{
    AppendStream, DeleteStream, ProcessTarget, ReadStream, ReadStreamCompleted, StreamTarget,
    SubscribeTo, SubscriptionRequestOutcome, SubscriptionTarget,
};
use crate::process::storage::StorageService;
pub use crate::process::subscriptions::SubscriptionsClient;
use crate::process::write_request_manager::WriteRequestManagerClient;
use geth_common::{
    AppendStreamCompleted, Client, DeleteStreamCompleted, Direction, ExpectedRevision,
    GetProgramError, ProgramKillError, ProgramKilled, ProgramObtained, ProgramSummary, Propose,
    Record, Revision, SubscriptionConfirmation, SubscriptionEvent,
};
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::chunks::ChunkContainer;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};

#[cfg(test)]
mod tests;

mod echo;
pub mod grpc;
pub mod indexing;
pub mod reading;
mod sink;
pub mod storage;
pub mod subscription;
mod subscriptions;
mod write_request_manager;
pub mod writing;

pub struct InternalClient<WAL, S: Storage> {
    storage: StorageService<WAL, S>,
    subscriptions: SubscriptionsClient,
    write_request_manager_client: WriteRequestManagerClient,
}

impl<WAL, S> InternalClient<WAL, S>
where
    S: Storage + Send + Sync + 'static,
    WAL: WriteAheadLog + Send + Sync + 'static,
{
    pub fn new(processes: Processes<WAL, S>) -> Self {
        Self {
            storage: processes.storage,
            subscriptions: processes.subscriptions,
            write_request_manager_client: processes.write_request_manager_client,
        }
    }
}

#[async_trait::async_trait]
impl<WAL, S> Client for InternalClient<WAL, S>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    async fn append_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
        proposes: Vec<Propose>,
    ) -> eyre::Result<AppendStreamCompleted> {
        let outcome = self
            .storage
            .append_stream(AppendStream {
                stream_name: stream_id.to_string(),
                events: proposes,
                expected: expected_revision,
            })
            .await?;

        if let AppendStreamCompleted::Success(ref result) = outcome {
            self.write_request_manager_client
                .wait_until_indexing_reach(result.position.raw())
                .await?;
        }

        Ok(outcome)
    }

    async fn read_stream(
        &self,
        stream_id: &str,
        direction: Direction,
        revision: Revision<u64>,
        max_count: u64,
    ) -> BoxStream<'static, eyre::Result<Record>> {
        let outcome = self
            .storage
            .read_stream(ReadStream {
                correlation: Default::default(),
                stream_name: stream_id.to_string(),
                direction,
                starting: revision,
                count: max_count as usize,
            })
            .await;

        let stream_id = stream_id.to_string();
        // // FIXME: should return first class error.
        // Box::pin(async_stream::try_stream! {
        //     match outcome {
        //         ReadStreamCompleted::StreamDeleted => {
        //             let () = Err(eyre::eyre!("stream '{}' deleted", stream_id))?;
        //
        //         } ,
        //         ReadStreamCompleted::Success(mut stream) => {
        //             while let Some(record) = stream.next().await? {
        //                 yield record;
        //             }
        //         }
        //         ReadStreamCompleted::Unexpected(e) => {
        //             let () = Err(e)?;
        //         }
        //     }
        // })
        todo!()
    }

    async fn subscribe_to_stream(
        &self,
        stream_id: &str,
        start: Revision<u64>,
    ) -> BoxStream<'static, eyre::Result<SubscriptionEvent>> {
        let catchup_stream = if start == Revision::End {
            None
        } else {
            Some(
                self.read_stream(stream_id, Direction::Forward, start, u64::MAX)
                    .await,
            )
        };

        let (sender, recv) = oneshot::channel();
        let outcome = self.subscriptions.subscribe(SubscribeMsg {
            payload: SubscribeTo {
                target: SubscriptionTarget::Stream(StreamTarget {
                    parent: None,
                    stream_name: stream_id.to_string(),
                }),
            },
            mail: sender,
        });

        let stream_id = stream_id.to_string();
        Box::pin(async_stream::try_stream! {
            outcome?;
            let resp = recv.await.map_err(|_| eyre::eyre!("Main bus has shutdown!"))?;

            let mut threshold = start.raw();
            let mut pre_read_events = false;
            if let Some(mut catchup_stream) = catchup_stream {
                while let Some(record) = catchup_stream.try_next().await? {
                    pre_read_events = true;
                    let revision = record.revision;
                    yield SubscriptionEvent::EventAppeared(record);
                    threshold = revision;
                }

                yield SubscriptionEvent::CaughtUp;
            }

            match resp.outcome {
                SubscriptionRequestOutcome::Success(mut stream) => {
                    yield SubscriptionEvent::Confirmed(SubscriptionConfirmation::StreamName(stream_id));

                    while let Some(record) = stream.next().await? {
                        if pre_read_events && record.revision <= threshold {
                            continue;
                        }

                        yield SubscriptionEvent::EventAppeared(record);
                    }
                }
                SubscriptionRequestOutcome::Failure(e) => {
                    let () = Err(e)?;
                }
            }
        })
    }

    async fn subscribe_to_process(
        &self,
        name: &str,
        source_code: &str,
    ) -> BoxStream<'static, eyre::Result<SubscriptionEvent>> {
        let (sender, recv) = oneshot::channel();
        let outcome = self.subscriptions.subscribe(SubscribeMsg {
            payload: SubscribeTo {
                target: SubscriptionTarget::Process(ProcessTarget {
                    id: Uuid::new_v4(),
                    name: name.to_string(),
                    source_code: source_code.to_string(),
                }),
            },
            mail: sender,
        });

        Box::pin(async_stream::try_stream! {
            outcome?;
            let resp = recv.await.map_err(|_| eyre::eyre!("Main bus has shutdown!"))?;

            match resp.outcome {
                SubscriptionRequestOutcome::Success(mut stream) => {
                    while let Some(record) = stream.next().await? {
                        yield SubscriptionEvent::EventAppeared(record);
                    }
                }
                SubscriptionRequestOutcome::Failure(e) => {
                    let () = Err(e)?;
                }
            }
        })
    }

    async fn delete_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
    ) -> eyre::Result<DeleteStreamCompleted> {
        let outcome = self
            .storage
            .delete_stream(DeleteStream {
                stream_name: stream_id.to_string(),
                expected: expected_revision,
            })
            .await?;

        Ok(outcome)
    }

    async fn list_programs(&self) -> eyre::Result<Vec<ProgramSummary>> {
        let (sender, recv) = oneshot::channel();
        if self
            .subscriptions
            .list_programmable_subscriptions(sender)
            .await
            .is_err()
        {
            eyre::bail!("Main bus has shutdown!");
        }

        if let Ok(resp) = recv.await {
            return Ok(resp);
        }

        eyre::bail!("Main bus has shutdown!");
    }

    async fn get_program(&self, id: Uuid) -> eyre::Result<ProgramObtained> {
        let (sender, recv) = oneshot::channel();
        if self
            .subscriptions
            .get_programmable_subscription_stats(GetProgrammableSubscriptionStatsMsg {
                id,
                mail: sender,
            })
            .await
            .is_err()
        {
            eyre::bail!("Main bus has shutdown!");
        }

        if let Ok(resp) = recv.await {
            return Ok(resp.map_or(
                ProgramObtained::Error(GetProgramError::NotExists),
                ProgramObtained::Success,
            ));
        }

        eyre::bail!("Main bus has shutdown!");
    }

    async fn kill_program(&self, id: Uuid) -> eyre::Result<ProgramKilled> {
        let (sender, recv) = oneshot::channel();
        if self
            .subscriptions
            .kill_programmable_subscription(KillProgrammableSubscriptionMsg { id, mail: sender })
            .await
            .is_err()
        {
            eyre::bail!("Main bus has shutdown!");
        }

        if let Ok(resp) = recv.await {
            return Ok(
                resp.map_or(ProgramKilled::Error(ProgramKillError::NotExists), |_| {
                    ProgramKilled::Success
                }),
            );
        }

        eyre::bail!("Main bus has shutdown!");
    }
}

#[derive(Clone)]
pub struct Processes<WAL, S>
where
    S: Storage,
{
    storage: StorageService<WAL, S>,
    subscriptions: SubscriptionsClient,
    write_request_manager_client: WriteRequestManagerClient,
}

impl<WAL, S> Processes<WAL, S>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    pub fn new(wal: WALRef<WAL>, index: IndexRef<S>) -> Self {
        let subscriptions = subscriptions::start();
        let write_request_manager_client = write_request_manager::start(subscriptions.clone());
        let storage = StorageService::new(wal, index, subscriptions.clone());

        Self {
            storage,
            subscriptions,
            write_request_manager_client,
        }
    }

    pub fn subscriptions_client(&self) -> &SubscriptionsClient {
        &self.subscriptions
    }
}

#[derive(Clone)]
pub enum Mailbox {
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

#[derive(Clone)]
pub struct Mail {
    pub origin: ProcId,
    pub correlation: Uuid,
    pub payload: Bytes,
    pub created: Instant,
}

impl Mail {}

#[derive(Clone)]
pub struct Process {
    pub id: ProcId,
    pub parent: Option<ProcId>,
    pub name: &'static str,
    pub mailbox: Mailbox,
    pub created: Instant,
}

pub struct Manager<S> {
    runtime: Runtime<S>,
    catalog: Catalog,
    proc_id_gen: ProcId,
    sender: UnboundedSender<ManagerCommand>,
    requests: HashMap<Uuid, oneshot::Sender<Mail>>,
    buffer: BytesMut,
    queue: UnboundedReceiver<ManagerCommand>,
    closing: bool,
    close_resp: Vec<oneshot::Sender<()>>,
    processes_shutting_down: usize,
}

#[derive(Clone, Copy, PartialOrd, PartialEq, Eq, Ord, Hash, Debug)]
pub enum Proc {
    Writing,
    Reading,
    Indexing,
    PubSub,
    Grpc,
    Echo,
    Sink,
}

impl Proc {
    // fn spawn(&self) {
    //     match self {
    //         Activator::Managed(builder) => {
    //             let toto = builder();
    //             let name = toto.name();
    //             let toto = builder();
    //         }
    //
    //         Activator::Raw(_) => {}
    //     }
    // }
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

struct Catalog {
    inner: HashMap<Proc, CatalogItem>,
    monitor: HashMap<ProcId, RunningProc>,
}

impl Catalog {
    fn lookup(&self, proc: &Proc) -> eyre::Result<Option<ProcId>> {
        if let Some(item) = self.inner.get(proc) {
            return match &item.topology {
                Topology::Singleton(prev) => Ok(prev.clone()),
            };
        }

        eyre::bail!("process {:?} is not registered", proc);
    }

    fn report(&mut self, running: RunningProc) -> eyre::Result<()> {
        let proc = running.proc;
        if let Some(item) = self.inner.get_mut(&running.proc) {
            match &mut item.topology {
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
            if let Some(item) = self.inner.get_mut(&running.proc) {
                match &mut item.topology {
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
    inner: HashMap<Proc, CatalogItem>,
}

impl CatalogBuilder {
    pub fn register(mut self, proc: Proc) -> Self {
        self.inner.insert(
            proc,
            CatalogItem {
                proc,
                topology: Topology::Singleton(None),
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

struct CatalogItem {
    proc: Proc,
    topology: Topology,
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
            ManagerCommand::Spawn { parent, resp } => {
                // TODO - might change that command to start.
            }

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
                    let sender = self.sender.clone();
                    let buffer = self.buffer.split();

                    let running_proc = match proc {
                        Proc::Writing => spawn_raw(
                            sender,
                            runtime,
                            buffer,
                            Handle::current(),
                            id,
                            proc,
                            writing::run,
                        ),

                        Proc::Reading => spawn_raw(
                            sender,
                            runtime,
                            buffer,
                            Handle::current(),
                            id,
                            proc,
                            reading::run,
                        ),

                        Proc::Indexing => spawn_raw(
                            sender,
                            runtime,
                            buffer,
                            Handle::current(),
                            id,
                            proc,
                            indexing::run,
                        ),

                        Proc::PubSub => spawn(sender, buffer, id, proc, subscription::run),
                        Proc::Grpc => spawn(sender, buffer, id, proc, grpc::run),
                        Proc::Echo => spawn(sender, buffer, id, proc, echo::run),
                        Proc::Sink => spawn(sender, buffer, id, proc, sink::run),
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

struct Stream {
    origin: ProcId,
    correlation: Uuid,
    payload: Bytes,
    sender: UnboundedSender<Bytes>,
    created: Instant,
}

enum Item {
    Mail(Mail),
    Stream(Stream),
}

impl Item {
    fn into_mail(self) -> Mail {
        if let Item::Mail(mail) = self {
            return mail;
        }

        panic!("item was not a mail");
    }
}

pub enum ManagerCommand {
    Spawn {
        parent: Option<ProcId>,
        resp: oneshot::Sender<ProcId>,
    },

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
        match self {
            ManagerCommand::ProcTerminated { .. } | ManagerCommand::Shutdown { .. } => true,
            _ => false,
        }
    }
}

pub struct ProcessEnv {
    id: ProcId,
    queue: UnboundedReceiver<Item>,
    client: ManagerClient,
    buffer: BytesMut,
}

pub struct ProcessRawEnv {
    id: ProcId,
    queue: std::sync::mpsc::Receiver<Item>,
    client: ManagerClient,
    buffer: BytesMut,
    handle: Handle,
}

#[derive(Clone)]
pub struct ManagerClient {
    id: ProcId,
    inner: UnboundedSender<ManagerCommand>,
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

    pub fn send(&self, dest: ProcId, payload: Bytes) -> eyre::Result<()> {
        self.send_with_correlation(dest, Uuid::new_v4(), payload)
    }

    pub fn send_with_correlation(
        &self,
        dest: ProcId,
        correlation: Uuid,
        payload: Bytes,
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

    pub async fn request(&self, dest: ProcId, payload: Bytes) -> eyre::Result<Mail> {
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
        payload: Bytes,
    ) -> eyre::Result<UnboundedReceiver<Bytes>> {
        let (sender, receiver) = unbounded_channel();
        if self
            .inner
            .send(ManagerCommand::Send {
                dest,
                item: Item::Stream(Stream {
                    origin: self.id,
                    correlation: Uuid::new_v4(),
                    created: Instant::now(),
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

    pub fn reply(&self, dest: ProcId, correlation: Uuid, payload: Bytes) -> eyre::Result<()> {
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

        match receiver.await {
            Ok(id) => Ok(id),
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
}

pub fn start_process_manager<S>(storage: S) -> ManagerClient
where
    S: Storage + Send + Sync + 'static,
{
    let catalog = Catalog::builder()
        .register(Proc::Indexing)
        .register(Proc::Writing)
        .register(Proc::Reading)
        .register(Proc::PubSub)
        .build();

    start_process_manager_with_catalog(storage, catalog).unwrap()
}

pub fn start_process_manager_with_catalog<S>(
    storage: S,
    catalog: Catalog,
) -> io::Result<ManagerClient>
where
    S: Storage + Send + Sync + 'static,
{
    let (sender, queue) = unbounded_channel();
    let mut buffer = BytesMut::new();
    let container = ChunkContainer::load(storage, &mut buffer)?;
    tokio::spawn(process_manager(
        catalog,
        container,
        buffer,
        sender.clone(),
        queue,
    ));

    Ok(ManagerClient {
        id: 0,
        inner: sender,
    })
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
    catalog: Catalog,
    container: ChunkContainer<S>,
    buffer: BytesMut,
    sender: UnboundedSender<ManagerCommand>,
    queue: UnboundedReceiver<ManagerCommand>,
) where
    S: Storage + Send + Sync + 'static,
{
    let mut manager = Manager {
        runtime: Runtime { container },
        catalog,
        proc_id_gen: 1,
        sender,
        requests: Default::default(),
        buffer,
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
    sender: UnboundedSender<ManagerCommand>,
    runtime: Runtime<S>,
    buffer: BytesMut,
    handle: Handle,
    id: ProcId,
    proc: Proc,
    runnable: F,
) -> RunningProc
where
    S: Storage + Send + Sync + 'static,
    F: FnOnce(Runtime<S>, ProcessRawEnv) -> eyre::Result<()> + Send + Sync + 'static,
{
    let (proc_sender, proc_queue) = std::sync::mpsc::channel();
    let env = ProcessRawEnv {
        id,
        queue: proc_queue,
        client: ManagerClient {
            id,
            inner: sender.clone(),
        },
        buffer,
        handle,
    };

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

fn spawn<F, Fut>(
    sender: UnboundedSender<ManagerCommand>,
    buffer: BytesMut,
    id: ProcId,
    proc: Proc,
    runnable: F,
) -> RunningProc
where
    F: FnOnce(ProcessEnv) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = eyre::Result<()>> + Send + 'static,
{
    let (proc_sender, proc_queue) = unbounded_channel();
    let env = ProcessEnv {
        id,
        queue: proc_queue,
        client: ManagerClient {
            id,
            inner: sender.clone(),
        },
        buffer,
    };

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
