use tokio::sync::oneshot;
use uuid::Uuid;

use geth_common::ProgramStats;

use crate::messages::{SubscribeTo, SubscriptionConfirmed};

pub struct SubscribeMsg {
    pub payload: SubscribeTo,
    pub mail: oneshot::Sender<SubscriptionConfirmed>,
}

pub struct GetProgrammableSubscriptionStatsMsg {
    pub id: Uuid,
    pub mail: oneshot::Sender<Option<ProgramStats>>,
}

pub struct KillProgrammableSubscriptionMsg {
    pub id: Uuid,
    pub mail: oneshot::Sender<Option<()>>,
}
