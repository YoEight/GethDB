use crate::domain::index::CurrentRevision;
use crate::process::indexing::IndexClient;
use crate::process::writing::{Request, Response};
use crate::process::{Item, ProcessRawEnv, RunnableRaw};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use geth_common::{ExpectedRevision, WrongExpectedRevisionError};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::wal::WriteAheadLog;

pub struct Writing<WAL> {
    wal: WAL,
}

impl<WAL> RunnableRaw for Writing<WAL>
where
    WAL: WriteAheadLog + 'static,
{
    fn name(&self) -> &'static str {
        "writer"
    }

    fn run(mut self: Box<Self>, mut env: ProcessRawEnv) -> eyre::Result<()> {
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
                    if let Some(req) = Request::try_from(mail.payload) {
                        let key = mikoshi_hash(req.ident());
                        let current_revision =
                            env.handle.block_on(index_client.latest_revision(key))?;

                        if let Some(e) =
                            optimistic_concurrency_check(req.expected(), current_revision)
                        {
                            env.client.reply(
                                mail.origin,
                                mail.correlation,
                                Response::wrong_expected_revision(e.expected, e.current)
                                    .serialize(&mut env.buffer),
                            )?;

                            continue;
                        }

                        match req {
                            Request::Append { events, .. } => {}

                            Request::Delete { ident, expected } => {}
                        }

                        continue;
                    }

                    tracing::error!("unhandled mail request {}", mail.correlation);
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

struct LogEntries {
    buffer: BytesMut,
    data: Bytes,
    ident: Bytes,
    revision: u64,
}

impl Iterator for LogEntries {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.data.has_remaining() {
            return None;
        }

        self.buffer.put_u16_le(self.ident.len() as u16);
        self.buffer.extend_from_slice(&self.ident);
        self.buffer.put_u64_le(self.revision);
        self.buffer.extend_from_slice(&self.data);

        self.revision += 1;

        Some(self.buffer.split().freeze())
    }
}
