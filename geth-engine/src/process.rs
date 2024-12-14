use std::collections::HashMap;
use std::time::Instant;
use std::{io, thread};

use bytes::{Bytes, BytesMut};
use eyre::bail;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use uuid::Uuid;

use geth_common::{
    AppendStreamCompleted, Client, DeleteStreamCompleted, Direction, ExpectedRevision,
    GetProgramError, ProgramKillError, ProgramKilled, ProgramObtained, ProgramSummary, Propose,
    Record, Revision, SubscriptionConfirmation, SubscriptionEvent,
};
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};

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

#[cfg(test)]
mod tests;

pub mod indexing;
pub mod storage;
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
        // FIXME: should return first class error.
        Box::pin(async_stream::try_stream! {
            match outcome {
                ReadStreamCompleted::StreamDeleted => {
                    let () = Err(eyre::eyre!("stream '{}' deleted", stream_id))?;

                } ,
                ReadStreamCompleted::Success(mut stream) => {
                    while let Some(record) = stream.next().await? {
                        yield record;
                    }
                }
                ReadStreamCompleted::Unexpected(e) => {
                    let () = Err(e)?;
                }
            }
        })
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
            bail!("Main bus has shutdown!");
        }

        if let Ok(resp) = recv.await {
            return Ok(resp);
        }

        bail!("Main bus has shutdown!");
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
            bail!("Main bus has shutdown!");
        }

        if let Ok(resp) = recv.await {
            return Ok(resp.map_or(
                ProgramObtained::Error(GetProgramError::NotExists),
                ProgramObtained::Success,
            ));
        }

        bail!("Main bus has shutdown!");
    }

    async fn kill_program(&self, id: Uuid) -> eyre::Result<ProgramKilled> {
        let (sender, recv) = oneshot::channel();
        if self
            .subscriptions
            .kill_programmable_subscription(KillProgrammableSubscriptionMsg { id, mail: sender })
            .await
            .is_err()
        {
            bail!("Main bus has shutdown!");
        }

        if let Ok(resp) = recv.await {
            return Ok(
                resp.map_or(ProgramKilled::Error(ProgramKillError::NotExists), |_| {
                    ProgramKilled::Success
                }),
            );
        }

        bail!("Main bus has shutdown!");
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

#[derive(Clone)]
pub struct Mail {
    pub origin: Uuid,
    pub correlation: Uuid,
    pub payload: Bytes,
    pub created: Instant,
}

impl Mail {}

#[derive(Clone)]
pub struct Process {
    pub id: Uuid,
    pub parent: Option<Uuid>,
    pub name: &'static str,
    pub mailbox: Mailbox,
    pub created: Instant,
}

#[derive(Clone, Copy, PartialOrd, PartialEq, Ord, Eq, Hash)]
struct ExchangeId {
    dest: Uuid,
    correlation: Uuid,
}

impl ExchangeId {
    fn new(dest: Uuid, correlation: Uuid) -> Self {
        Self { dest, correlation }
    }
}

pub struct Manager {
    processes: HashMap<Uuid, Process>,
    sender: UnboundedSender<ManagerCommand>,
    requests: HashMap<ExchangeId, oneshot::Sender<Mail>>,
    wait_fors: HashMap<&'static str, Vec<oneshot::Sender<Uuid>>>,
    buffer: BytesMut,
    queue: UnboundedReceiver<ManagerCommand>,
    closing: bool,
    close_resp: Vec<oneshot::Sender<()>>,
    processes_shutting_down: usize,
}

impl Manager {
    fn handle(&mut self, cmd: ManagerCommand) {
        if self.closing && !cmd.is_shutdown_related() {
            return;
        }

        match cmd {
            ManagerCommand::Spawn {
                parent,
                runnable,
                resp,
            } => {
                let id = Uuid::new_v4();

                let proc = match runnable {
                    RunnableType::Tokio(runnable) => {
                        let (proc_sender, proc_queue) = unbounded_channel();
                        let name = runnable.name();
                        let manager_sender = self.sender.clone();
                        let buffer = self.buffer.split();

                        tokio::spawn(async move {
                            let error = runnable
                                .run(ProcessEnv {
                                    id,
                                    queue: proc_queue,
                                    client: ManagerClient {
                                        id,
                                        parent,
                                        inner: manager_sender.clone(),
                                    },
                                    buffer,
                                })
                                .await
                                .err();

                            let _ =
                                manager_sender.send(ManagerCommand::ProcTerminated { id, error });
                        });

                        Process {
                            id,
                            parent: None,
                            mailbox: Mailbox::Tokio(proc_sender),
                            name,
                            created: Instant::now(),
                        }
                    }

                    RunnableType::Raw(runnable) => {
                        let (proc_sender, proc_queue) = std::sync::mpsc::channel();
                        let manager_client = self.sender.clone();
                        let manager_buffer = self.buffer.split();
                        let name = runnable.name();
                        let handle = Handle::current();

                        thread::spawn(move || {
                            let error = runnable
                                .run(ProcessRawEnv {
                                    id,
                                    queue: proc_queue,
                                    client: ManagerClient {
                                        id,
                                        parent,
                                        inner: manager_client.clone(),
                                    },
                                    buffer: manager_buffer,
                                    handle,
                                })
                                .err();

                            let _ =
                                manager_client.send(ManagerCommand::ProcTerminated { id, error });
                        });

                        Process {
                            id,
                            parent: None,
                            mailbox: Mailbox::Raw(proc_sender),
                            name,
                            created: Instant::now(),
                        }
                    }
                };

                tracing::info!("process {}:{} has started", proc.name, id);
                let _ = resp.send(id);
                self.processes.insert(id, proc);
            }

            ManagerCommand::Find { name, resp } => {
                for proc in self.processes.values() {
                    if proc.name == name {
                        let _ = resp.send(Some(proc.id));
                        return;
                    }
                }

                let _ = resp.send(None);
            }

            ManagerCommand::Send { dest, item, resp } => match item {
                Item::Mail(mail) => {
                    if let Some(resp) = self
                        .requests
                        .remove(&ExchangeId::new(mail.origin, mail.correlation))
                    {
                        let _ = resp.send(mail);
                    } else if let Some(proc) = self.processes.get(&dest) {
                        if let Some(resp) = resp {
                            self.requests
                                .insert(ExchangeId::new(dest, mail.correlation), resp);
                        }

                        let _ = proc.mailbox.send(Item::Mail(mail));
                    }
                }

                Item::Stream(stream) => {
                    if let Some(proc) = self.processes.get_mut(&dest) {
                        let _ = proc.mailbox.send(Item::Stream(stream));
                    }
                }
            },

            ManagerCommand::WaitFor { name, resp } => {
                for proc in self.processes.values() {
                    if proc.name != name {
                        continue;
                    }

                    let _ = resp.send(proc.id);
                    return;
                }

                if let Some(backlog) = self.wait_fors.get_mut(name) {
                    backlog.push(resp);
                } else {
                    self.wait_fors.insert(name, vec![resp]);
                }
            }

            ManagerCommand::ProcTerminated { id, error } => {
                if let Some(proc) = self.processes.remove(&id) {
                    if let Some(e) = error {
                        tracing::error!("process {}:{} terminated with error {}", proc.name, id, e);
                    } else {
                        tracing::info!("process {}:{} terminated", proc.name, id);
                    }

                    tracing::debug!(
                        "proceeding to terminate all child-processes related to {}:{}",
                        proc.name,
                        id
                    );

                    let mut ids = vec![];
                    for (child_id, child) in &self.processes {
                        if child.parent == Some(id) {
                            ids.push(*child_id);
                        }
                    }

                    for id in ids {
                        if let Some(child) = self.processes.remove(&id) {
                            tracing::info!("terminated child-process {}:{}", child.name, id);
                        }
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
                    self.processes_shutting_down = self.processes.len();

                    if self.processes_shutting_down == 0 {
                        let _ = resp.send(());
                        self.queue.close();
                        return;
                    }

                    self.processes.clear();
                }

                self.close_resp.push(resp);
            }
        }
    }
}

struct Stream {
    origin: Uuid,
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

enum RunnableType {
    Tokio(Box<dyn Runnable + Send + Sync + 'static>),
    Raw(Box<dyn RunnableRaw + Send + Sync + 'static>),
}

impl RunnableType {
    fn name(&self) -> &'static str {
        match self {
            RunnableType::Tokio(x) => x.name(),
            RunnableType::Raw(x) => x.name(),
        }
    }
}

pub enum ManagerCommand {
    Spawn {
        parent: Option<Uuid>,
        runnable: RunnableType,
        resp: oneshot::Sender<Uuid>,
    },

    Find {
        name: &'static str,
        resp: oneshot::Sender<Option<Uuid>>,
    },

    Send {
        dest: Uuid,
        item: Item,
        resp: Option<oneshot::Sender<Mail>>,
    },

    WaitFor {
        name: &'static str,
        resp: oneshot::Sender<Uuid>,
    },

    ProcTerminated {
        id: Uuid,
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
    id: Uuid,
    queue: UnboundedReceiver<Item>,
    client: ManagerClient,
    buffer: BytesMut,
}

pub struct ProcessRawEnv {
    id: Uuid,
    queue: std::sync::mpsc::Receiver<Item>,
    client: ManagerClient,
    buffer: BytesMut,
    handle: Handle,
}

#[async_trait::async_trait]
pub trait Runnable {
    fn name(&self) -> &'static str;
    async fn run(self: Box<Self>, env: ProcessEnv) -> eyre::Result<()>;
}

pub trait RunnableRaw {
    fn name(&self) -> &'static str;
    fn run(self: Box<Self>, env: ProcessRawEnv) -> eyre::Result<()>;
}

#[derive(Clone)]
pub struct ManagerClient {
    id: Uuid,
    parent: Option<Uuid>,
    inner: UnboundedSender<ManagerCommand>,
}

impl ManagerClient {
    pub async fn spawn<R>(&self, run: R) -> eyre::Result<Uuid>
    where
        R: Runnable + Send + Sync + 'static,
    {
        let (resp, receiver) = oneshot::channel();
        if self
            .inner
            .send(ManagerCommand::Spawn {
                parent: self.parent,
                runnable: RunnableType::Tokio(Box::new(run)),
                resp,
            })
            .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        match receiver.await {
            Ok(id) => Ok(id),
            Err(_) => eyre::bail!("process manager has shutdown"),
        }
    }

    pub async fn spawn_raw<R>(&self, run: R) -> eyre::Result<Uuid>
    where
        R: RunnableRaw + Send + Sync + 'static,
    {
        let (resp, receiver) = oneshot::channel();
        if self
            .inner
            .send(ManagerCommand::Spawn {
                parent: self.parent,
                runnable: RunnableType::Raw(Box::new(run)),
                resp,
            })
            .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        match receiver.await {
            Ok(id) => Ok(id),
            Err(_) => eyre::bail!("process manager has shutdown"),
        }
    }

    pub async fn find(&self, name: &'static str) -> eyre::Result<Option<Uuid>> {
        let (resp, receiver) = oneshot::channel();
        if self
            .inner
            .send(ManagerCommand::Find { name, resp })
            .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        match receiver.await {
            Ok(id) => Ok(id),
            Err(_) => eyre::bail!("process manager has shutdown"),
        }
    }

    pub fn send(&self, dest: Uuid, payload: Bytes) -> eyre::Result<()> {
        self.send_with_correlation(dest, Uuid::new_v4(), payload)
    }

    pub fn send_with_correlation(
        &self,
        dest: Uuid,
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

    pub async fn request(&self, dest: Uuid, payload: Bytes) -> eyre::Result<Mail> {
        let (resp, receiver) = oneshot::channel();
        if self
            .inner
            .send(ManagerCommand::Send {
                dest,
                item: Item::Mail(Mail {
                    origin: self.id,
                    correlation: Default::default(),
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
        dest: Uuid,
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

    pub fn reply(&self, dest: Uuid, correlation: Uuid, payload: Bytes) -> eyre::Result<()> {
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

    pub async fn wait_for(&self, name: &'static str) -> eyre::Result<Uuid> {
        let (resp, receiver) = oneshot::channel();
        if self
            .inner
            .send(ManagerCommand::WaitFor { name, resp })
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

pub fn start_process_manager() -> ManagerClient {
    let (sender, queue) = unbounded_channel();
    tokio::spawn(process_manager(sender.clone(), queue));

    ManagerClient {
        id: Uuid::new_v4(),
        parent: None,
        inner: sender,
    }
}

async fn process_manager(
    sender: UnboundedSender<ManagerCommand>,
    queue: UnboundedReceiver<ManagerCommand>,
) {
    let mut manager = Manager {
        processes: Default::default(),
        sender,
        requests: Default::default(),
        wait_fors: Default::default(),
        buffer: BytesMut::new(),
        closing: false,
        close_resp: vec![],
        queue,
        processes_shutting_down: 0,
    };

    while let Some(cmd) = manager.queue.recv().await {
        manager.handle(cmd);
    }
}
