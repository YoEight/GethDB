use std::fmt::Display;

use futures_util::stream::BoxStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

use geth_common::{
    AppendStream, AppendStreamCompleted, Client, DeleteStream, DeleteStreamCompleted, Direction,
    EndPoint, ExpectedRevision, GetProgram, KillProgram, ListPrograms, Operation, ProgramKilled,
    ProgramObtained, ProgramStats, ProgramSummary, Propose, ReadStream, Record, Reply, Revision,
    StreamRead, Subscribe, SubscribeToProgram, SubscribeToStream, SubscriptionEvent,
    UnsubscribeReason,
};

use crate::next::{Command, Msg, multiplex_loop, OperationIn, OperationOut};
use crate::next::driver::Driver;

pub struct Task {
    rx: UnboundedReceiver<OperationOut>,
}

impl Task {
    // FIXME: There would be some general error at that level.
    pub async fn recv(&mut self) -> eyre::Result<Option<Reply>> {
        if let Some(event) = self.rx.recv().await {
            return Ok(Some(event.reply));
        }

        Ok(None)
    }
}

#[derive(Clone)]
pub struct Mailbox {
    tx: UnboundedSender<Msg>,
}

impl Mailbox {
    pub fn new(tx: UnboundedSender<Msg>) -> Self {
        Self { tx }
    }

    async fn send(&self, msg: Msg) -> eyre::Result<()> {
        self.tx
            .send(msg)
            .map_err(|_| eyre::eyre!("connection is permanently closed"))?;

        Ok(())
    }

    pub async fn send_operation(&self, operation: Operation) -> eyre::Result<Task> {
        let (resp, rx) = mpsc::unbounded_channel();

        self.send(Msg::Command(Command {
            operation_in: OperationIn {
                correlation: Uuid::new_v4(),
                operation,
            },
            resp,
        }))
        .await?;

        Ok(Task { rx })
    }
}

#[derive(Clone)]
pub struct GrpcClient {
    mailbox: Mailbox,
}

impl GrpcClient {
    pub fn new(endpoint: EndPoint) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let driver = Driver::new(endpoint, tx.clone());

        tokio::spawn(multiplex_loop(driver, rx));

        Self {
            mailbox: Mailbox::new(tx),
        }
    }
}

#[async_trait::async_trait]
impl Client for GrpcClient {
    async fn append_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
        proposes: Vec<Propose>,
    ) -> eyre::Result<AppendStreamCompleted> {
        let mut task = self
            .mailbox
            .send_operation(Operation::AppendStream(AppendStream {
                stream_name: stream_id.to_string(),
                events: proposes,
                expected_revision,
            }))
            .await?;

        if let Some(out) = task.recv().await? {
            match out {
                Reply::AppendStreamCompleted(resp) => Ok(resp),
                _ => eyre::bail!("unexpected reply when appending events to '{}'", stream_id),
            }
        } else {
            eyre::bail!(
                "unexpected code path when appending events to '{}'",
                stream_id
            );
        }
    }

    async fn read_stream(
        &self,
        stream_id: &str,
        direction: Direction,
        revision: Revision<u64>,
        max_count: u64,
    ) -> BoxStream<'static, eyre::Result<Record>> {
        let outcome = self
            .mailbox
            .send_operation(Operation::ReadStream(ReadStream {
                stream_name: stream_id.to_string(),
                direction,
                revision,
                max_count,
            }))
            .await;

        let stream_id = stream_id.to_string();
        Box::pin(async_stream::try_stream! {
            let mut task = outcome?;
            while let Some(event) = task.recv().await? {
                match event {
                    Reply::StreamRead(read) => match read {
                        StreamRead::EventAppeared(record) => {
                            yield record;
                        }

                        StreamRead::Error(e) => {
                            read_error(&stream_id, e)?;
                        }

                        StreamRead::EndOfStream => break,
                    }

                    _ => {
                        unexpected_reply_when_reading(&stream_id)?;
                    }
                }
            }
        })
    }

    async fn subscribe_to_stream(
        &self,
        stream_id: &str,
        start: Revision<u64>,
    ) -> BoxStream<'static, eyre::Result<SubscriptionEvent>> {
        let outcome = self
            .mailbox
            .send_operation(Operation::Subscribe(Subscribe::ToStream(
                SubscribeToStream {
                    stream_name: stream_id.to_string(),
                    start,
                },
            )))
            .await;

        produce_subscription_stream(stream_id.to_string(), outcome)
    }

    async fn subscribe_to_process(
        &self,
        name: &str,
        source_code: &str,
    ) -> BoxStream<'static, eyre::Result<SubscriptionEvent>> {
        let outcome = self
            .mailbox
            .send_operation(Operation::Subscribe(Subscribe::ToProgram(
                SubscribeToProgram {
                    name: name.to_string(),
                    source: source_code.to_string(),
                },
            )))
            .await;

        produce_subscription_stream(format!("process '{}'", name), outcome)
    }

    async fn delete_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
    ) -> eyre::Result<DeleteStreamCompleted> {
        let mut task = self
            .mailbox
            .send_operation(Operation::DeleteStream(DeleteStream {
                stream_name: stream_id.to_string(),
                expected_revision,
            }))
            .await?;

        if let Some(event) = task.recv().await? {
            match event {
                Reply::DeleteStreamCompleted(resp) => return Ok(resp),
                _ => eyre::bail!("unexpected reply when deleting stream '{}'", stream_id),
            }
        }

        eyre::bail!("unexpected code path when deleting stream '{}'", stream_id);
    }

    async fn list_programs(&self) -> eyre::Result<Vec<ProgramSummary>> {
        let mut task = self
            .mailbox
            .send_operation(Operation::ListPrograms(ListPrograms {}))
            .await?;

        if let Some(event) = task.recv().await? {
            match event {
                Reply::ProgramsListed(resp) => {
                    return Ok(resp.programs);
                }
                _ => eyre::bail!("unexpected reply when listing programs"),
            }
        }

        eyre::bail!("unexpected code path when listing programs");
    }

    async fn get_program(&self, id: Uuid) -> eyre::Result<ProgramStats> {
        let mut task = self
            .mailbox
            .send_operation(Operation::GetProgram(GetProgram { id }))
            .await?;

        if let Some(event) = task.recv().await? {
            match event {
                Reply::ProgramObtained(resp) => {
                    return match resp {
                        ProgramObtained::Error(e) => Err(e),
                        ProgramObtained::Success(stats) => Ok(stats),
                    }
                }
                _ => eyre::bail!("unexpected reply when getting program {}", id),
            }
        }

        eyre::bail!("unexpected code path when getting program {}", id);
    }

    async fn kill_program(&self, id: Uuid) -> eyre::Result<()> {
        let mut task = self
            .mailbox
            .send_operation(Operation::KillProgram(KillProgram { id }))
            .await?;

        if let Some(event) = task.recv().await? {
            match event {
                Reply::ProgramKilled(resp) => {
                    return match resp {
                        ProgramKilled::Error(e) => Err(e),
                        ProgramKilled::Success => Ok(()),
                    }
                }
                _ => eyre::bail!("unexpected reply when killing program {}", id),
            }
        }

        eyre::bail!("unexpected code path when killing program {}", id);
    }
}

fn produce_subscription_stream<'a>(
    ident: String,
    outcome: eyre::Result<Task>,
) -> BoxStream<'a, eyre::Result<SubscriptionEvent>> {
    Box::pin(async_stream::try_stream! {
        let mut task = outcome?;
        while let Some(event) = task.recv().await? {
            match event {
                Reply::SubscriptionEvent(event) => {
                    yield event;
                }

                _ => unexpected_reply_when_reading(&ident)?,
            }
        }

        yield SubscriptionEvent::Unsubscribed(UnsubscribeReason::User);
    })
}

fn unexpected_reply_when_reading(stream_id: &str) -> eyre::Result<()> {
    eyre::bail!("unexpected reply when reading: {}", stream_id)
}

fn read_error<T: Display>(stream_id: &str, e: T) -> eyre::Result<()> {
    eyre::bail!("error when reading '{}': {}", stream_id, e)
}
