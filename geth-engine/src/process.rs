use std::collections::HashMap;
use std::time::Instant;

use bytes::Bytes;
use eyre::bail;
use futures::stream::BoxStream;
use futures::TryStreamExt;
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

pub mod indexing;
pub mod storage;
mod subscriptions;
mod write_request_manager;

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

pub type Mailbox = UnboundedSender<Mail>;

#[derive(Clone)]
pub struct Mail {
    pub origin: Uuid,
    pub correlation: Uuid,
    pub payload: Bytes,
    pub created: Instant,
}

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
}

impl Manager {
    fn handle(&mut self, cmd: ManagerCommand) {
        match cmd {
            ManagerCommand::Spawn { parent, runnable } => {
                let (proc_sender, proc_queue) = unbounded_channel();

                let id = Uuid::new_v4();
                let proc = Process {
                    id,
                    parent: None,
                    mailbox: proc_sender,
                    name: runnable.name(),
                    created: Instant::now(),
                };

                self.processes.insert(id, proc);
                tokio::spawn(runnable.run(
                    id,
                    proc_queue,
                    ManagerClient {
                        parent: Some(id),
                        inner: self.sender.clone(),
                    },
                ));
            }

            ManagerCommand::Find { name, resp } => {
                for proc in self.processes.values() {
                    if proc.name == name {
                        let _ = resp.send(Some(proc.id));
                        break;
                    }
                }
            }

            ManagerCommand::Send { dest, mail, resp } => {
                if let Some(resp) = self
                    .requests
                    .remove(&ExchangeId::new(dest, mail.correlation))
                {
                    let _ = resp.send(mail);
                } else if let Some(proc) = self.processes.get(&dest) {
                    if let Some(resp) = resp {
                        self.requests
                            .insert(ExchangeId::new(dest, mail.correlation), resp);
                    }

                    let _ = proc.mailbox.send(mail);
                }
            }

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

            ManagerCommand::Shutdown { resp } => {
                // TODO - implement the shutdown logic.
                let _ = resp.send(());
            }
        }
    }
}

pub enum ManagerCommand {
    Spawn {
        parent: Option<Uuid>,
        runnable: Box<dyn Runnable + Send + Sync + 'static>,
    },

    Find {
        name: &'static str,
        resp: oneshot::Sender<Option<Uuid>>,
    },

    Send {
        dest: Uuid,
        mail: Mail,
        resp: Option<oneshot::Sender<Mail>>,
    },

    WaitFor {
        name: &'static str,
        resp: oneshot::Sender<Uuid>,
    },

    Shutdown {
        resp: oneshot::Sender<()>,
    },
}

#[async_trait::async_trait]
pub trait Runnable {
    fn name(&self) -> &'static str;
    async fn run(self: Box<Self>, id: Uuid, queue: UnboundedReceiver<Mail>, client: ManagerClient);
}

#[derive(Clone)]
pub struct ManagerClient {
    parent: Option<Uuid>,
    inner: UnboundedSender<ManagerCommand>,
}

impl ManagerClient {
    pub fn spawn<R>(&self, run: R)
    where
        R: Runnable + Send + Sync + 'static,
    {
        let _ = self.inner.send(ManagerCommand::Spawn {
            parent: self.parent,
            runnable: Box::new(run),
        });
    }

    pub async fn find(&self, name: &'static str) -> Option<Uuid> {
        let (resp, recv) = oneshot::channel();
        let _ = self.inner.send(ManagerCommand::Find { name, resp });

        recv.await.unwrap()
    }

    pub fn send(&self, dest: Uuid, mail: Mail) {
        let _ = self.inner.send(ManagerCommand::Send {
            dest,
            mail,
            resp: None,
        });
    }

    pub async fn send_and_wait(&self, dest: Uuid, mail: Mail) -> Mail {
        let (resp, recv) = oneshot::channel();
        let _ = self.inner.send(ManagerCommand::Send {
            dest,
            mail,
            resp: Some(resp),
        });

        recv.await.unwrap()
    }

    pub async fn wait_for(&self, name: &'static str) -> Uuid {
        let (resp, recv) = oneshot::channel();
        let _ = self.inner.send(ManagerCommand::WaitFor { name, resp });

        recv.await.unwrap()
    }

    pub async fn shutdown(&self) {
        let (resp, recv) = oneshot::channel();
        let _ = self.inner.send(ManagerCommand::Shutdown { resp });

        recv.await.unwrap()
    }
}

pub fn start_process_manager(procs: Vec<Box<dyn Runnable + Send + Sync + 'static>>) {
    tokio::spawn(process_manager(procs));
}

async fn process_manager(runnables: Vec<Box<dyn Runnable + Send + Sync + 'static>>) {
    let (sender, mut queue) = unbounded_channel();
    let mut manager = Manager {
        processes: Default::default(),
        sender,
        requests: Default::default(),
        wait_fors: Default::default(),
    };

    for runnable in runnables {
        manager.handle(ManagerCommand::Spawn {
            parent: None,
            runnable,
        });
    }

    while let Some(cmd) = queue.recv().await {
        manager.handle(cmd);
    }
}
