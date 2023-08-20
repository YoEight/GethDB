use eyre::bail;
use geth_common::{ProgrammableStats, ProgrammableSummary};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};
use uuid::Uuid;

use crate::messages::{
    AppendStream, AppendStreamCompleted, DeleteStream, DeleteStreamCompleted, ReadStream,
    ReadStreamCompleted, SubscribeTo, SubscriptionConfirmed,
};

pub enum Msg {
    ReadStream(ReadStreamMsg),
    AppendStream(AppendStreamMsg),
    DeleteStream(DeleteStreamMsg),
    Subscribe(SubscribeMsg),
    GetProgrammableSubscriptionStats(GetProgrammableSubscriptionStatsMsg),
    KillProgrammableSubscription(KillProgrammableSubscriptionMsg),
    ListProgrammableSubscriptions(ListProgrammableSubscriptionsMsg),
}

pub struct ReadStreamMsg {
    pub payload: ReadStream,
    pub mail: oneshot::Sender<eyre::Result<ReadStreamCompleted>>,
}

pub struct AppendStreamMsg {
    pub payload: AppendStream,
    pub mail: oneshot::Sender<eyre::Result<AppendStreamCompleted>>,
}

pub struct DeleteStreamMsg {
    pub payload: DeleteStream,
    pub mail: oneshot::Sender<eyre::Result<DeleteStreamCompleted>>,
}

pub struct SubscribeMsg {
    pub payload: SubscribeTo,
    pub mail: oneshot::Sender<SubscriptionConfirmed>,
}

pub struct GetProgrammableSubscriptionStatsMsg {
    pub id: Uuid,
    pub mail: oneshot::Sender<Option<ProgrammableStats>>,
}

pub struct KillProgrammableSubscriptionMsg {
    pub id: Uuid,
    pub mail: oneshot::Sender<()>,
}

pub struct ListProgrammableSubscriptionsMsg {
    pub mail: oneshot::Sender<Vec<ProgrammableSummary>>,
}

#[derive(Clone)]
pub struct Bus {
    inner: Sender<Msg>,
}

impl Bus {
    pub async fn read_stream(&self, msg: ReadStream) -> eyre::Result<ReadStreamCompleted> {
        let (sender, recv) = oneshot::channel();
        if self
            .inner
            .send(Msg::ReadStream(ReadStreamMsg {
                payload: msg,
                mail: sender,
            }))
            .await
            .is_err()
        {
            bail!("Main bus has shutdown!");
        }

        if let Ok(resp) = recv.await {
            return resp;
        }

        bail!("Main bus has shutdown!");
    }

    pub async fn append_stream(&self, msg: AppendStream) -> eyre::Result<AppendStreamCompleted> {
        let (sender, recv) = oneshot::channel();
        if self
            .inner
            .send(Msg::AppendStream(AppendStreamMsg {
                payload: msg,
                mail: sender,
            }))
            .await
            .is_err()
        {
            bail!("Main bus has shutdown!");
        }

        if let Ok(resp) = recv.await {
            return resp;
        }

        bail!("Main bus has shutdown!");
    }

    pub async fn delete_stream(&self, msg: DeleteStream) -> eyre::Result<DeleteStreamCompleted> {
        let (sender, recv) = oneshot::channel();
        if self
            .inner
            .send(Msg::DeleteStream(DeleteStreamMsg {
                payload: msg,
                mail: sender,
            }))
            .await
            .is_err()
        {
            bail!("Main bus has shutdown!");
        }

        if let Ok(resp) = recv.await {
            return resp;
        }

        bail!("Main bus has shutdown!");
    }

    pub async fn subscribe_to(&self, msg: SubscribeTo) -> eyre::Result<SubscriptionConfirmed> {
        let (sender, recv) = oneshot::channel();
        if self
            .inner
            .send(Msg::Subscribe(SubscribeMsg {
                payload: msg,
                mail: sender,
            }))
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

    pub async fn get_programmable_subscription_stats(
        &self,
        id: Uuid,
    ) -> eyre::Result<Option<ProgrammableStats>> {
        let (sender, recv) = oneshot::channel();
        if self
            .inner
            .send(Msg::GetProgrammableSubscriptionStats(
                GetProgrammableSubscriptionStatsMsg { id, mail: sender },
            ))
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
            .inner
            .send(Msg::KillProgrammableSubscription(
                KillProgrammableSubscriptionMsg { id, mail: sender },
            ))
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
            .inner
            .send(Msg::ListProgrammableSubscriptions(
                ListProgrammableSubscriptionsMsg { mail: sender },
            ))
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

pub struct Mailbox {
    inner: Receiver<Msg>,
}

impl Mailbox {
    pub async fn next(&mut self) -> Option<Msg> {
        self.inner.recv().await
    }
}

pub fn new_bus(buffer: usize) -> (Bus, Mailbox) {
    let (sender, recv) = mpsc::channel(buffer);

    (Bus { inner: sender }, Mailbox { inner: recv })
}
