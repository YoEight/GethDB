use crate::domain::index::CurrentRevision;
use crate::process::indexing::IndexClient;
use crate::process::writing::{Request, Response};
use crate::process::{Item, ProcessRawEnv, RunnableRaw};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use geth_common::{ExpectedRevision, WrongExpectedRevisionError};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::storage::{FileId, Storage};
use geth_mikoshi::wal::chunks::ChunkContainer;
use geth_mikoshi::wal::{LogEntries, LogWriter, WriteAheadLog};
use std::io;

pub struct Writing<S> {
    container: ChunkContainer<S>,
}

impl<S> Writing<S> {
    pub fn new(container: ChunkContainer<S>) -> Self {
        Self { container }
    }
}

impl<S> RunnableRaw for Writing<S>
where
    S: Storage + 'static,
{
    fn name(&self) -> &'static str {
        "writer"
    }

    fn run(mut self: Box<Self>, mut env: ProcessRawEnv) -> eyre::Result<()> {
        let mut log_writer = LogWriter::load(self.container, env.buffer.split())?;
        let mut index_client = IndexClient::resolve_raw(&mut env)?;

        while let Some(item) = env.queue.recv().ok() {
            match item {
                Item::Stream(stream) => {
                    tracing::error!(
                        "request {}: writer process doesn't support any stream operation",
                        stream.correlation
                    );

                    continue;
                }

                Item::Mail(mail) => {
                    if let Some(Request::Append {
                        ident,
                        expected,
                        index,
                        events,
                    }) = Request::try_from(mail.payload)
                    {
                        let key = mikoshi_hash(&ident);
                        let current_revision =
                            env.handle.block_on(index_client.latest_revision(key))?;

                        if current_revision.is_deleted() {
                            env.client.reply(
                                mail.origin,
                                mail.correlation,
                                Response::Deleted.serialize(&mut env.buffer),
                            )?;

                            continue;
                        }

                        if let Some(e) = optimistic_concurrency_check(expected, current_revision) {
                            env.client.reply(
                                mail.origin,
                                mail.correlation,
                                Response::wrong_expected_revision(e.expected, e.current)
                                    .serialize(&mut env.buffer),
                            )?;

                            continue;
                        }

                        let revision = current_revision.next_revision();
                        let mut entries = LogEntries::new(ident, revision, index, events);
                        let receipt = log_writer.append(&mut entries)?;

                        if index {
                            env.handle
                                .block_on(index_client.store(key, entries.complete()))?;
                        }

                        env.client.reply(
                            mail.origin,
                            mail.correlation,
                            Response::committed(receipt.start_position, receipt.next_position)
                                .serialize(&mut env.buffer),
                        )?;

                        continue;
                    }

                    tracing::warn!("unhandled mail request {}", mail.correlation);
                }
            }
        }

        Ok(())
    }
}

fn optimistic_concurrency_check(
    expected: ExpectedRevision,
    current: CurrentRevision,
) -> Option<WrongExpectedRevisionError> {
    match (expected, current) {
        (ExpectedRevision::NoStream, CurrentRevision::NoStream) => None,
        (ExpectedRevision::StreamExists, CurrentRevision::Revision(_)) => None,
        (ExpectedRevision::Any, _) => None,
        (ExpectedRevision::Revision(a), CurrentRevision::Revision(b)) if a == b => None,
        _ => Some(WrongExpectedRevisionError {
            expected,
            current: current.as_expected(),
        }),
    }
}
