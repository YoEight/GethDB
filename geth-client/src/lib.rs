use std::sync::Arc;

use futures_util::TryStreamExt;
use geth_common::{
    AppendStreamCompleted, DeleteStreamCompleted, Direction, ExpectedRevision, ProgramStats,
    ProgramSummary, Propose, ReadStreamCompleted, ReadStreamResponse, Record, Revision,
    SubscriptionConfirmation, SubscriptionEvent,
};
pub use next::grpc::GrpcClient;
use tonic::Streaming;

mod next;
mod types;

pub enum ReadStreaming {
    Grpc(Streaming<geth_common::protocol::ReadStreamResponse>),
    Local(geth_engine::reading::Streaming),
    Subscription(SubscriptionStreaming),
}

impl ReadStreaming {
    pub async fn next(&mut self) -> eyre::Result<Option<Record>> {
        match self {
            ReadStreaming::Grpc(streaming) => {
                if let Some(resp) = streaming.try_next().await? {
                    match resp.into() {
                        ReadStreamResponse::EventAppeared(record) => return Ok(Some(record)),
                        ReadStreamResponse::EndOfStream => return Ok(None),
                        ReadStreamResponse::StreamDeleted => unreachable!(),
                    }
                }

                Ok(None)
            }

            ReadStreaming::Local(streaming) => streaming.next().await,

            ReadStreaming::Subscription(sub) => {
                while let Some(event) = sub.next().await? {
                    match event {
                        SubscriptionEvent::EventAppeared(record) => return Ok(Some(record)),
                        SubscriptionEvent::Confirmed(_) | SubscriptionEvent::CaughtUp => continue,
                        SubscriptionEvent::Unsubscribed(_) => break,
                    }
                }

                Ok(None)
            }
        }
    }
}

enum SubscriptionType {
    Grpc(Streaming<geth_common::protocol::SubscribeResponse>),
}

pub struct SubscriptionStreaming {
    confirmation: Option<SubscriptionConfirmation>,
    r#type: SubscriptionType,
}

impl SubscriptionStreaming {
    pub fn from_grpc(streaming: Streaming<geth_common::protocol::SubscribeResponse>) -> Self {
        Self {
            confirmation: None,
            r#type: SubscriptionType::Grpc(streaming),
        }
    }

    pub async fn wait_until_confirmed(&mut self) -> eyre::Result<SubscriptionConfirmation> {
        if let Some(conf) = self.confirmation.as_ref() {
            return Ok(conf.clone());
        }

        if let Some(SubscriptionEvent::Confirmed(conf)) = self.next().await? {
            self.confirmation = Some(conf.clone());
            return Ok(conf);
        }

        eyre::bail!("subcription was never confirmed")
    }

    pub async fn next(&mut self) -> eyre::Result<Option<SubscriptionEvent>> {
        match &mut self.r#type {
            SubscriptionType::Grpc(streaming) => {
                if let Some(resp) = streaming.try_next().await? {
                    return Ok(Some(resp.into()));
                }

                Ok(None)
            }
        }
    }
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
    ) -> eyre::Result<ReadStreamCompleted<ReadStreaming>>;

    async fn subscribe_to_stream(
        &self,
        stream_id: &str,
        start: Revision<u64>,
    ) -> eyre::Result<SubscriptionStreaming>;

    async fn subscribe_to_process(
        &self,
        name: &str,
        source_code: &str,
    ) -> eyre::Result<SubscriptionStreaming>;

    async fn delete_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
    ) -> eyre::Result<DeleteStreamCompleted>;

    async fn list_programs(&self) -> eyre::Result<Vec<ProgramSummary>>;

    async fn get_program(&self, id: u64) -> eyre::Result<Option<ProgramStats>>;

    async fn stop_program(&self, id: u64) -> eyre::Result<()>;
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
    ) -> eyre::Result<ReadStreamCompleted<ReadStreaming>> {
        self.as_ref()
            .read_stream(stream_id, direction, revision, max_count)
            .await
    }

    async fn subscribe_to_stream(
        &self,
        stream_id: &str,
        start: Revision<u64>,
    ) -> eyre::Result<SubscriptionStreaming> {
        self.as_ref().subscribe_to_stream(stream_id, start).await
    }

    async fn subscribe_to_process(
        &self,
        name: &str,
        source_code: &str,
    ) -> eyre::Result<SubscriptionStreaming> {
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

    async fn get_program(&self, id: u64) -> eyre::Result<Option<ProgramStats>> {
        self.as_ref().get_program(id).await
    }

    async fn stop_program(&self, id: u64) -> eyre::Result<()> {
        self.as_ref().stop_program(id).await
    }
}
