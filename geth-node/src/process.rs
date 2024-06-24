use std::io;

use eyre::bail;
use futures::Stream;
use tokio::sync::oneshot;
use uuid::Uuid;

use geth_common::{
    Client, Direction, ExpectedRevision, ProgramStats, ProgramSummary, ProgrammableStats,
    ProgrammableSummary, Propose, Record, Revision, SubscriptionEvent, WriteResult,
};
use geth_domain::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};

use crate::bus::{
    GetProgrammableSubscriptionStatsMsg, KillProgrammableSubscriptionMsg, SubscribeMsg,
};
use crate::messages::{
    AppendStream, AppendStreamCompleted, DeleteStream, DeleteStreamCompleted, ProcessTarget,
    ReadStream, ReadStreamCompleted, StreamTarget, SubscribeTo, SubscriptionConfirmed,
    SubscriptionRequestOutcome, SubscriptionTarget,
};
use crate::process::storage::StorageService;
use crate::process::subscriptions::SubscriptionsClient;

pub mod storage;
mod subscriptions;

pub struct InternalClient<WAL, S: Storage> {
    storage: StorageService<WAL, S>,
    subscriptions: SubscriptionsClient,
}

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
    ) -> eyre::Result<WriteResult> {
        let outcome = self
            .storage
            .append_stream(AppendStream {
                correlation: Default::default(),
                stream_name: stream_id.to_string(),
                events: proposes,
                expected: expected_revision,
            })
            .await?;

        // FIXME: should return first class error.
        match outcome {
            AppendStreamCompleted::Success(r) => Ok(r),
            AppendStreamCompleted::Failure(e) => bail!("error: {:?}", e),
            AppendStreamCompleted::Unexpected(e) => Err(e),
            AppendStreamCompleted::StreamDeleted => bail!("stream '{}' deleted", stream_id),
        }
    }

    async fn read_stream(
        &self,
        stream_id: &str,
        direction: Direction,
        revision: Revision<u64>,
        max_count: u64,
    ) -> impl Stream<Item = eyre::Result<Record>> {
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

        // FIXME: should return first class error.
        async_stream::try_stream! {
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
        }
    }

    async fn subscribe_to_stream(
        &self,
        stream_id: &str,
        start: Revision<u64>,
    ) -> impl Stream<Item = eyre::Result<SubscriptionEvent>> {
        let (sender, recv) = oneshot::channel();
        let outcome = self.subscriptions.subscribe(SubscribeMsg {
            payload: SubscribeTo {
                correlation: Uuid::new_v4(),
                target: SubscriptionTarget::Stream(StreamTarget {
                    parent: None,
                    stream_name: stream_id.to_string(),
                    starting: start,
                }),
            },
            mail: sender,
        });

        async_stream::try_stream! {
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
        }
    }

    async fn subscribe_to_process(
        &self,
        name: &str,
        source_code: &str,
    ) -> impl Stream<Item = eyre::Result<SubscriptionEvent>> {
        let (sender, recv) = oneshot::channel();
        let outcome = self.subscriptions.subscribe(SubscribeMsg {
            payload: SubscribeTo {
                correlation: Uuid::new_v4(),
                target: SubscriptionTarget::Process(ProcessTarget {
                    id: Uuid::new_v4(),
                    name: name.to_string(),
                    source_code: source_code.to_string(),
                }),
            },
            mail: sender,
        });

        async_stream::try_stream! {
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
        }
    }

    async fn delete_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
    ) -> eyre::Result<WriteResult> {
        let outcome = self
            .storage
            .delete_stream(DeleteStream {
                stream_name: stream_id.to_string(),
                expected: expected_revision,
            })
            .await?;

        // FIXME - should return first class error.
        match outcome {
            DeleteStreamCompleted::Success(r) => Ok(r),
            DeleteStreamCompleted::Failure(e) => bail!("error: {:?}", e),
            DeleteStreamCompleted::Unexpected(e) => Err(e),
        }
    }

    async fn list_programs(&self) -> eyre::Result<Vec<ProgramSummary>> {
        todo!()
    }

    async fn get_program(&self, id: Uuid) -> eyre::Result<ProgramStats> {
        todo!()
    }

    async fn kill_program(&self, id: Uuid) -> eyre::Result<()> {
        todo!()
    }
}

#[derive(Clone)]
pub struct Processes<WAL, S>
where
    S: Storage,
{
    storage: StorageService<WAL, S>,
    subscriptions: SubscriptionsClient,
}

impl<WAL, S> Processes<WAL, S>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    pub fn new(wal: WALRef<WAL>, index: Lsm<S>) -> Self {
        let subscriptions = subscriptions::start();
        let storage = StorageService::new(wal, index, subscriptions.clone());

        Self {
            storage,
            subscriptions,
        }
    }

    pub async fn append_stream(&self, params: AppendStream) -> io::Result<AppendStreamCompleted> {
        self.storage.append_stream(params).await
    }

    pub async fn read_stream(&self, params: ReadStream) -> ReadStreamCompleted {
        self.storage.read_stream(params).await
    }

    pub async fn delete_stream(&self, params: DeleteStream) -> io::Result<DeleteStreamCompleted> {
        self.storage.delete_stream(params).await
    }

    pub async fn subscribe_to(&self, msg: SubscribeTo) -> eyre::Result<SubscriptionConfirmed> {
        let (sender, recv) = oneshot::channel();
        if self
            .subscriptions
            .subscribe(SubscribeMsg {
                payload: msg,
                mail: sender,
            })
            .is_err()
        {
            bail!("Main bus has shutdown!");
        }

        if let Ok(resp) = recv.await {
            return Ok(resp);
        }

        bail!("Main bus has shutdown!");
    }

    pub async fn get_programmable_subscription_stats(
        &self,
        id: Uuid,
    ) -> eyre::Result<Option<ProgrammableStats>> {
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
            return Ok(resp);
        }

        bail!("Main bus has shutdown!");
    }

    pub async fn kill_programmable_subscription(&self, id: Uuid) -> eyre::Result<()> {
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
            return Ok(resp);
        }

        bail!("Main bus has shutdown!");
    }

    pub async fn list_programmable_subscriptions(&self) -> eyre::Result<Vec<ProgrammableSummary>> {
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
}
