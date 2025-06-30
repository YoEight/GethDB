use geth_client::{Client, ReadStreaming, SubscriptionStreaming};
use geth_common::{
    AppendStreamCompleted, DeleteStreamCompleted, Direction, ExpectedRevision, ProgramStats,
    ProgramSummary, Propose, ReadStreamCompleted, Revision,
};
use geth_engine::{EmbeddedClient, Options, ReaderClient, RequestContext, WriterClient};

#[derive(Clone)]
pub struct LocalClient {
    client: EmbeddedClient,
    writer: WriterClient,
    reader: ReaderClient,
}

impl LocalClient {
    pub async fn new(options: Options) -> eyre::Result<Self> {
        let client = geth_engine::run_embedded(&options).await?;

        Ok(Self {
            writer: client.manager().new_writer_client().await?,
            reader: client.manager().new_reader_client().await?,
            client,
        })
    }

    pub async fn shutdown(self) -> eyre::Result<()> {
        self.client.shutdown().await
    }
}

#[async_trait::async_trait]
impl Client for LocalClient {
    async fn append_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
        proposes: Vec<Propose>,
    ) -> eyre::Result<AppendStreamCompleted> {
        self.writer
            .append(
                RequestContext::new(),
                stream_id.to_string(),
                expected_revision,
                proposes,
            )
            .await
    }

    async fn read_stream(
        &self,
        stream_id: &str,
        direction: Direction,
        revision: Revision<u64>,
        max_count: u64,
    ) -> eyre::Result<ReadStreamCompleted<ReadStreaming>> {
        let outcome = self
            .reader
            .read(
                RequestContext::new(),
                stream_id,
                revision,
                direction,
                max_count as usize,
            )
            .await?;

        match outcome {
            ReadStreamCompleted::StreamDeleted => Ok(ReadStreamCompleted::StreamDeleted),
            ReadStreamCompleted::Success(reading) => {
                Ok(ReadStreamCompleted::Success(ReadStreaming::Local(reading)))
            }
        }
    }

    async fn subscribe_to_stream(
        &self,
        _stream_id: &str,
        _start: Revision<u64>,
    ) -> eyre::Result<SubscriptionStreaming> {
        eyre::bail!("subscriptions are not supported in local mode");
    }

    async fn subscribe_to_process(
        &self,
        _name: &str,
        _source_code: &str,
    ) -> eyre::Result<SubscriptionStreaming> {
        eyre::bail!("subscriptions are not supported in local mode");
    }

    async fn delete_stream(
        &self,
        _stream_id: &str,
        _expected_revision: ExpectedRevision,
    ) -> eyre::Result<DeleteStreamCompleted> {
        eyre::bail!("not implemented")
    }

    async fn list_programs(&self) -> eyre::Result<Vec<ProgramSummary>> {
        eyre::bail!("not implemented")
    }

    async fn get_program(&self, _id: u64) -> eyre::Result<Option<ProgramStats>> {
        eyre::bail!("not implemented")
    }

    async fn stop_program(&self, _id: u64) -> eyre::Result<()> {
        eyre::bail!("not implemented")
    }
}
