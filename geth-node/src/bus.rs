use eyre::bail;
use geth_common::ProgrammableStats;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};
use uuid::Uuid;

use crate::messages::{
    AppendStream, AppendStreamCompleted, ReadStream, ReadStreamCompleted, SubscribeTo,
    SubscriptionConfirmed,
};

pub enum Msg {
    ReadStream(ReadStreamMsg),
    AppendStream(AppendStreamMsg),
    Subscribe(SubscribeMsg),
    GetProgrammableSubscriptionStats(GetProgrammableSubscriptionStatsMsg),
}

pub struct ReadStreamMsg {
    pub payload: ReadStream,
    pub mail: oneshot::Sender<ReadStreamCompleted>,
}

pub struct AppendStreamMsg {
    pub payload: AppendStream,
    pub mail: oneshot::Sender<AppendStreamCompleted>,
}

pub struct SubscribeMsg {
    pub payload: SubscribeTo,
    pub mail: oneshot::Sender<SubscriptionConfirmed>,
}

pub struct GetProgrammableSubscriptionStatsMsg {
    pub id: Uuid,
    pub mail: oneshot::Sender<Option<ProgrammableStats>>,
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
            return Ok(resp);
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
            return Ok(resp);
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
