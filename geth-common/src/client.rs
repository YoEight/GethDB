#![allow(async_fn_in_trait)]

use crate::{Record, SubscriptionConfirmation};

#[derive(Debug)]
pub enum SubscriptionEvent {
    EventAppeared(Record),
    Confirmed(SubscriptionConfirmation),
    CaughtUp,
    Unsubscribed(UnsubscribeReason),
}

impl SubscriptionEvent {
    pub fn is_event_appeared(&self) -> bool {
        if let Self::EventAppeared(_) = self {
            return true;
        }

        false
    }
}

#[derive(Debug)]
pub enum UnsubscribeReason {
    User,
    Server,
}
