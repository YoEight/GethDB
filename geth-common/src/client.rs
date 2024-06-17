#![allow(async_fn_in_trait)]

use futures::Stream;

use crate::{
    Direction, ExpectedRevision, Propose, Record, Revision, SubscriptionConfirmation,
    WriteResult,
};

pub enum SubscriptionEvent {
    EventAppeared(Record),
    Confirmed(SubscriptionConfirmation),
    CaughtUp,
    Unsubscribed(UnsubscribeReason),
}

pub enum UnsubscribeReason {
    User,
    Server,
}

pub trait Client {
    async fn append_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
        proposes: Vec<Propose>,
    ) -> eyre::Result<WriteResult>;

    fn read_stream(
        &self,
        stream_id: &str,
        direction: Direction,
        revision: Revision<u64>,
        max_count: u64,
    ) -> impl Stream<Item = eyre::Result<Record>>;

    fn subscribe_to_stream(
        &self,
        stream_id: &str,
        start: Revision<u64>,
    ) -> impl Stream<Item = eyre::Result<SubscriptionEvent>>;

    fn subscribe_to_process(
        &self,
        name: &str,
        source_code: &str,
    ) -> impl Stream<Item = eyre::Result<SubscriptionEvent>>;

    async fn delete_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
    ) -> eyre::Result<WriteResult>;
}
