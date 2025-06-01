use futures::stream::BoxStream;
use geth_common::{
    AppendStreamCompleted, Client, DeleteStreamCompleted, Direction, ExpectedRevision,
    ProgramKilled, ProgramObtained, ProgramSummary, Propose, ReadStreamCompleted, Record, Revision,
    SubscriptionEvent,
};
use geth_engine::{ManagerClient, Proc, ReaderClient, WriterClient};

#[derive(Clone)]
pub struct LocalClient {
    writer: WriterClient,
    reader: ReaderClient,
}

impl LocalClient {
    pub async fn new(client: ManagerClient) -> eyre::Result<Self> {
        let writer_id = client.wait_for(Proc::Writing).await?.must_succeed()?;
        let reader_id = client.wait_for(Proc::Reading).await?.must_succeed()?;

        Ok(Self {
            writer: WriterClient::new(writer_id, client.clone()),
            reader: ReaderClient::new(reader_id, client),
        })
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
            .append(stream_id.to_string(), expected_revision, proposes)
            .await
    }

    async fn read_stream(
        &self,
        stream_id: &str,
        direction: Direction,
        revision: Revision<u64>,
        max_count: u64,
    ) -> eyre::Result<ReadStreamCompleted<BoxStream<'static, eyre::Result<Record>>>> {
        let outcome = self
            .reader
            .read(stream_id, revision, direction, max_count as usize)
            .await?;

        match outcome {
            ReadStreamCompleted::StreamDeleted => Ok(ReadStreamCompleted::StreamDeleted),
            ReadStreamCompleted::Unexpected(e) => Ok(ReadStreamCompleted::Unexpected(e)),
            ReadStreamCompleted::Success(mut reading) => {
                let stream = async_stream::try_stream! {
                    while let Some(record) = reading.next().await? {
                        yield record;
                    }
                };

                Ok(ReadStreamCompleted::Success(Box::pin(stream)))
            }
        }
    }

    async fn subscribe_to_stream(
        &self,
        _stream_id: &str,
        _start: Revision<u64>,
    ) -> BoxStream<'static, eyre::Result<SubscriptionEvent>> {
        Box::pin(async_stream::stream! {
            yield Err(eyre::eyre!("subscriptions are not supported in local mode"));
        })
    }

    async fn subscribe_to_process(
        &self,
        _name: &str,
        _source_code: &str,
    ) -> BoxStream<'static, eyre::Result<SubscriptionEvent>> {
        Box::pin(async_stream::stream! {
            yield Err(eyre::eyre!("subscriptions are not supported in local mode"));
        })
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

    async fn get_program(&self, _id: u64) -> eyre::Result<ProgramObtained> {
        eyre::bail!("not implemented")
    }

    async fn kill_program(&self, _id: u64) -> eyre::Result<ProgramKilled> {
        eyre::bail!("not implemented")
    }
}
