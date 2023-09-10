use geth_common::ProgrammableStats;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::messages::{SubscribeTo, SubscriptionConfirmed};

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
