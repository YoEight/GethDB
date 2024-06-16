use std::fmt::Display;

use futures_util::Stream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

use geth_common::{
    Client, Direction, EndPoint, ExpectedRevision, Propose, ReadStream, Record, Revision,
    StreamRead, WriteResult,
};

use crate::next::driver::Driver;
use crate::next::{multiplex_loop, Command, Msg, Operation, Reply};

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
        todo!()
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
}

fn unexpected_reply_when_reading(stream_id: &str) -> eyre::Result<()> {
    eyre::bail!("unexpected reply when reading: {}", stream_id)
}

fn read_error<T: Display>(stream_id: &str, e: T) -> eyre::Result<()> {
    eyre::bail!("error when reading stream {}: {}", stream_id, e)
}
