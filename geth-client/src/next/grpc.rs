use std::fmt::Display;

use futures_util::Stream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

use geth_common::{
    AppendStream, AppendStreamCompleted, Client, DeleteStream, DeleteStreamCompleted, Direction,
    EndPoint, ExpectedRevision, GetProgram, ListPrograms, ProgramKilled, ProgramObtained,
    ProgramStats, ProgramSummary, Propose, ReadStream, Record, Revision, StreamRead, Subscribe,
    SubscribeToProgram, SubscribeToStream, SubscriptionEvent, SubscriptionEventIR,
    UnsubscribeReason, WriteResult,
};

use crate::next::{Command, Event, Msg, multiplex_loop, Operation, Reply};
use crate::next::driver::Driver;

pub struct GrpcClient {
    endpoint: EndPoint,
    mailbox: UnboundedSender<Msg>,
}

impl GrpcClient {
    pub fn new(endpoint: EndPoint) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let driver = Driver::new(endpoint.clone(), tx.clone());

        tokio::spawn(multiplex_loop(driver, rx));

        Self {
            endpoint,
            mailbox: tx,
        }
    }
}

impl Client for GrpcClient {
    async fn append_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
        proposes: Vec<Propose>,
    ) -> eyre::Result<WriteResult> {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let outcome = self.mailbox.send(Msg::Command(Command {
            correlation: Uuid::new_v4(),
            operation: Operation::AppendStream(AppendStream {
                stream_name: stream_id.to_string(),
                events: proposes,
                expected_revision,
            }),
            resp: tx,
        }));

        if outcome.is_err() {
            eyre::bail!("connection is permanently closed");
        }

        if let Some(event) = rx.recv().await {
            match event.reply {
                Reply::AppendStreamCompleted(resp) => match resp {
                    AppendStreamCompleted::WriteResult(result) => {
                        return Ok(result);
                    }
                    AppendStreamCompleted::Error(e) => {
                        eyre::bail!("error when appending events to '{}': {}", stream_id, e);
                    }
                },
                Reply::Errored => eyre::bail!("error when appending events to '{}'", stream_id),
                _ => eyre::bail!("unexpected reply when appending events to '{}'", stream_id),
            }
        } else {
            eyre::bail!(
                "unexpected code path when appending events to '{}'",
                stream_id
            );
        }
    }

    fn read_stream(
        &self,
        stream_id: &str,
        direction: Direction,
        revision: Revision<u64>,
        max_count: u64,
    ) -> impl Stream<Item = eyre::Result<Record>> {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let outcome = self.mailbox.send(Msg::Command(Command {
            correlation: Uuid::new_v4(),
            operation: Operation::ReadStream(ReadStream {
                stream_name: stream_id.to_string(),
                direction,
                revision,
                max_count,
            }),
            resp: tx,
        }));

        async_stream::try_stream! {
             if outcome.is_err() {
                read_error(stream_id, "connection is permanently closed")?;
            }

            while let Some(event) = rx.recv().await {
                match event.reply {
                    Reply::StreamRead(read) => match read {
                        StreamRead::EventsAppeared(records) => {
                            for record in records {
                                yield record;
                            }
                        }

                        StreamRead::Error(e) => {
                            read_error(stream_id, e)?;
                        }

                        StreamRead::EndOfStream => break,
                    }

                    Reply::Errored => {
                        read_error(stream_id, "")?;
                    }

                    _ => {
                        unexpected_reply_when_reading(stream_id)?;
                    }
                }
            }
        }
    }

    fn subscribe_to_stream(
        &self,
        stream_id: &str,
        start: Revision<u64>,
    ) -> impl Stream<Item = eyre::Result<SubscriptionEvent>> {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let outcome = self.mailbox.send(Msg::Command(Command {
            correlation: Uuid::new_v4(),
            operation: Operation::Subscribe(Subscribe::ToStream(SubscribeToStream {
                stream_name: stream_id.to_string(),
                start,
            })),
            resp: tx,
        }));

        let rx = outcome
            .map(|_| rx)
            .map_err(|e| eyre::eyre!("connection is permanently closed"));

        produce_subscription_stream(stream_id.to_string(), rx)
    }

    fn subscribe_to_process(
        &self,
        name: &str,
        source_code: &str,
    ) -> impl Stream<Item = eyre::Result<SubscriptionEvent>> {
        let (tx, rx) = mpsc::unbounded_channel();

        let outcome = self.mailbox.send(Msg::Command(Command {
            correlation: Uuid::new_v4(),
            operation: Operation::Subscribe(Subscribe::ToProgram(SubscribeToProgram {
                name: name.to_string(),
                source: source_code.to_string(),
            })),
            resp: tx,
        }));

        let rx = outcome
            .map(|_| rx)
            .map_err(|e| eyre::eyre!("connection is permanently closed"));

        produce_subscription_stream(format!("process '{}'", name), rx)
    }

    async fn delete_stream(
        &self,
        stream_id: &str,
        expected_revision: ExpectedRevision,
    ) -> eyre::Result<WriteResult> {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let outcome = self.mailbox.send(Msg::Command(Command {
            correlation: Uuid::new_v4(),
            operation: Operation::DeleteStream(DeleteStream {
                stream_name: stream_id.to_string(),
                expected_revision,
            }),
            resp: tx,
        }));

        if outcome.is_err() {
            eyre::bail!("connection is permanently closed");
        }

        if let Some(event) = rx.recv().await {
            match event.reply {
                Reply::DeleteStreamCompleted(resp) => match resp {
                    DeleteStreamCompleted::DeleteResult(result) => {
                        return Ok(result);
                    }
                    DeleteStreamCompleted::Error(e) => {
                        eyre::bail!("error when appending events to '{}': {}", stream_id, e);
                    }
                },
                Reply::Errored => eyre::bail!("error when appending events to '{}'", stream_id),
                _ => eyre::bail!("unexpected reply when appending events to '{}'", stream_id),
            }
        }

        eyre::bail!(
            "unexpected code path when appending events to '{}'",
            stream_id
        );
    }

    async fn list_programs(&self) -> eyre::Result<Vec<ProgramSummary>> {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let outcome = self.mailbox.send(Msg::Command(Command {
            correlation: Uuid::new_v4(),
            operation: Operation::ListPrograms(ListPrograms {}),
            resp: tx,
        }));

        if outcome.is_err() {
            eyre::bail!("connection is permanently closed");
        }

        if let Some(event) = rx.recv().await {
            match event.reply {
                Reply::ProgramsListed(resp) => {
                    return Ok(resp.programs);
                }
                Reply::Errored => eyre::bail!("error when listing programs"),
                _ => eyre::bail!("unexpected reply when listing programs"),
            }
        }

        eyre::bail!("unexpected code path when listing programs");
    }

    async fn get_program(&self, id: Uuid) -> eyre::Result<ProgramStats> {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let outcome = self.mailbox.send(Msg::Command(Command {
            correlation: Uuid::new_v4(),
            operation: Operation::GetProgram(GetProgram { id }),
            resp: tx,
        }));

        if outcome.is_err() {
            eyre::bail!("connection is permanently closed");
        }

        if let Some(event) = rx.recv().await {
            match event.reply {
                Reply::ProgramObtained(resp) => {
                    return match resp {
                        ProgramObtained::Error(e) => Err(e),
                        ProgramObtained::Success(stats) => Ok(stats),
                    }
                }
                Reply::Errored => eyre::bail!("error when getting program {}", id),
                _ => eyre::bail!("unexpected reply when getting program {}", id),
            }
        }

        eyre::bail!("unexpected code path when getting program {}", id);
    }

    async fn kill_program(&self, id: Uuid) -> eyre::Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let outcome = self.mailbox.send(Msg::Command(Command {
            correlation: Uuid::new_v4(),
            operation: Operation::GetProgram(GetProgram { id }),
            resp: tx,
        }));

        if outcome.is_err() {
            eyre::bail!("connection is permanently closed");
        }

        if let Some(event) = rx.recv().await {
            match event.reply {
                Reply::ProgramKilled(resp) => {
                    return match resp {
                        ProgramKilled::Error(e) => Err(e),
                        ProgramKilled::Success => Ok(()),
                    }
                }
                Reply::Errored => eyre::bail!("error when killing program {}", id),
                _ => eyre::bail!("unexpected reply when killing program {}", id),
            }
        }

        eyre::bail!("unexpected code path when killing program {}", id);
    }
}

fn produce_subscription_stream(
    ident: String,
    rx: eyre::Result<mpsc::UnboundedReceiver<Event>>,
) -> impl Stream<Item = eyre::Result<SubscriptionEvent>> {
    async_stream::try_stream! {
        let mut rx = rx?;
        while let Some(event) = rx.recv().await {
            match event.reply {
                Reply::SubscriptionEvent(event) => {
                    match event {
                        SubscriptionEventIR::EventsAppeared(events) => {
                            for record in events {
                                yield SubscriptionEvent::EventAppeared(record);
                            }
                        }

                        SubscriptionEventIR::Confirmation(confirm) => yield SubscriptionEvent::Confirmed(confirm),
                        SubscriptionEventIR::CaughtUp => yield SubscriptionEvent::CaughtUp,
                        SubscriptionEventIR::Error(e) => read_error(&ident, e)?,
                    }
                }

                Reply::Errored => read_error(&ident, "")?,

                _ => unexpected_reply_when_reading(&ident)?,
            }
        }

        yield SubscriptionEvent::Unsubscribed(UnsubscribeReason::User);
    }
}

fn unexpected_reply_when_reading(stream_id: &str) -> eyre::Result<()> {
    eyre::bail!("unexpected reply when reading: {}", stream_id)
}

fn read_error<T: Display>(stream_id: &str, e: T) -> eyre::Result<()> {
    eyre::bail!("error when reading '{}': {}", stream_id, e)
}
