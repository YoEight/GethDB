#![allow(async_fn_in_trait)]

use crate::{Record, SubscriptionConfirmation};

#[derive(Debug)]
pub enum SubscriptionEvent {
    EventAppeared(Record),
    Confirmed(SubscriptionConfirmation),
    CaughtUp,
    Unsubscribed(UnsubscribeReason),
}

#[derive(Debug)]
pub enum UnsubscribeReason {
    User,
    Server,
}
