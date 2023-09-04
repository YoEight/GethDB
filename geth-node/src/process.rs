use crate::bus::{
    GetProgrammableSubscriptionStatsMsg, KillProgrammableSubscriptionMsg, SubscribeMsg,
};
use crate::messages::{
    AppendStream, AppendStreamCompleted, DeleteStream, DeleteStreamCompleted, ReadStream,
    ReadStreamCompleted, SubscribeTo, SubscriptionConfirmed,
};
use crate::process::storage::StorageService;
use crate::process::subscriptions::SubscriptionsClient;
use eyre::bail;
use geth_common::{ProgrammableStats, ProgrammableSummary};
use geth_mikoshi::index::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};
use tokio::sync::oneshot;
use uuid::Uuid;

mod storage;
mod subscriptions;

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

    pub async fn append_stream(&self, params: AppendStream) -> AppendStreamCompleted {
        self.storage.append_stream(params).await
    }

    pub async fn read_stream(&self, params: ReadStream) -> ReadStreamCompleted {
        self.storage.read_stream(params).await
    }

    pub async fn delete_stream(&self, params: DeleteStream) -> DeleteStreamCompleted {
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
