#![allow(async_fn_in_trait)]

use std::sync::Arc;

use futures::stream::BoxStream;
use uuid::Uuid;

use crate::{
    AppendStreamCompleted, DeleteStreamCompleted, Direction, ExpectedRevision, ProgramKilled,
    ProgramObtained, ProgramSummary, Propose, ReadStreamCompleted, Record, Revision,
    SubscriptionConfirmation,
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
    ) -> eyre::Result<AppendStreamCompleted>;

    async fn read_stream(
        &self,
        stream_id: &str,
        direction: Direction,
        revision: Revision<u64>,
        max_count: u64,
    ) -> eyre::Result<ReadStreamCompleted<BoxStream<'static, eyre::Result<Record>>>>;

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
    ) -> eyre::Result<DeleteStreamCompleted>;

    async fn list_programs(&self) -> eyre::Result<Vec<ProgramSummary>>;

    async fn get_program(&self, id: Uuid) -> eyre::Result<ProgramObtained>;

    async fn kill_program(&self, id: Uuid) -> eyre::Result<ProgramKilled>;
}

#[async_trait::async_trait]
impl<C> Client for Arc<C>
where
    C: Client + Send + Sync,
{
    async fn append_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
        proposes: Vec<Propose>,
    ) -> eyre::Result<AppendStreamCompleted> {
        self.as_ref()
            .append_stream(stream_id, expected_revision, proposes)
            .await
    }

    async fn read_stream(
        &self,
        stream_id: &str,
        direction: Direction,
        revision: Revision<u64>,
        max_count: u64,
    ) -> eyre::Result<ReadStreamCompleted<BoxStream<'static, eyre::Result<Record>>>> {
        self.as_ref()
            .read_stream(stream_id, direction, revision, max_count)
            .await
    }

    async fn subscribe_to_stream(
        &self,
        stream_id: &str,
        start: Revision<u64>,
    ) -> BoxStream<'static, eyre::Result<SubscriptionEvent>> {
        self.as_ref().subscribe_to_stream(stream_id, start).await
    }

    async fn subscribe_to_process(
        &self,
        name: &str,
        source_code: &str,
    ) -> BoxStream<'static, eyre::Result<SubscriptionEvent>> {
        self.as_ref().subscribe_to_process(name, source_code).await
    }

    async fn delete_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
    ) -> eyre::Result<DeleteStreamCompleted> {
        self.as_ref()
            .delete_stream(stream_id, expected_revision)
            .await
    }

    async fn list_programs(&self) -> eyre::Result<Vec<ProgramSummary>> {
        self.as_ref().list_programs().await
    }

    async fn get_program(&self, id: Uuid) -> eyre::Result<ProgramObtained> {
        self.as_ref().get_program(id).await
    }

    async fn kill_program(&self, id: Uuid) -> eyre::Result<ProgramKilled> {
        self.as_ref().kill_program(id).await
    }
}
