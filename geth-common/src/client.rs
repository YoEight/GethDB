#![allow(async_fn_in_trait)]

use futures::stream::BoxStream;
use uuid::Uuid;

use crate::{
    Direction, ExpectedRevision, ProgramStats, ProgramSummary, Propose, Record, Revision,
    SubscriptionConfirmation, WriteResult,
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

#[async_trait::async_trait]
pub trait Client {
    async fn append_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
        proposes: Vec<Propose>,
    ) -> eyre::Result<WriteResult>;

    async fn read_stream(
        &self,
        stream_id: &str,
        direction: Direction,
        revision: Revision<u64>,
        max_count: u64,
    ) -> BoxStream<'static, eyre::Result<Record>>;

    async fn subscribe_to_stream(
        &self,
        stream_id: &str,
        start: Revision<u64>,
    ) -> BoxStream<'static, eyre::Result<SubscriptionEvent>>;

    async fn subscribe_to_process(
        &self,
        name: &str,
        source_code: &str,
    ) -> BoxStream<'static, eyre::Result<SubscriptionEvent>>;

    async fn delete_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
    ) -> eyre::Result<WriteResult>;

    async fn list_programs(&self) -> eyre::Result<Vec<ProgramSummary>>;

    async fn get_program(&self, id: Uuid) -> eyre::Result<ProgramStats>;

    async fn kill_program(&self, id: Uuid) -> eyre::Result<()>;
}
