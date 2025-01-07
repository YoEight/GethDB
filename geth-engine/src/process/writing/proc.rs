use crate::domain::index::CurrentRevision;
use crate::names::types::STREAM_DELETED;
use crate::process::indexing::IndexClient;
use crate::process::messages::{WriteRequests, WriteResponses};
use crate::process::subscription::SubscriptionClient;
use crate::process::{Item, ProcessRawEnv, Runtime};
use bytes::Bytes;
use geth_common::{ContentType, ExpectedRevision, Propose, WrongExpectedRevisionError};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::LogWriter;
use uuid::Uuid;

use super::entries::ProposeEntries;

pub fn run<S>(runtime: Runtime<S>, env: ProcessRawEnv) -> eyre::Result<()>
where
    S: Storage + 'static,
{
    let pool = env.client.pool.clone();
    let mut buffer = env.handle.block_on(pool.get()).unwrap();
    let mut log_writer = LogWriter::load(runtime.container().clone(), buffer.split())?;
    let mut index_client = IndexClient::resolve_raw(&env)?;
    let sub_client = SubscriptionClient::resolve_raw(&env)?;
    std::mem::drop(buffer);

    while let Ok(item) = env.queue.recv() {
        match item {
            Item::Stream(stream) => {
                tracing::error!(
                    "request {}: writer process doesn't support any stream operation",
                    stream.correlation
                );

                continue;
            }

            Item::Mail(mail) => {
                if let Ok(req) = mail.payload.try_into() {
                    match req {
                        WriteRequests::Write {
                            ident,
                            expected,
                            events,
                        } => {
                            let key = mikoshi_hash(&ident);
                            let current_revision =
                                env.handle.block_on(index_client.latest_revision(key))?;

                            if current_revision.is_deleted() {
                                env.client.reply(
                                    mail.origin,
                                    mail.correlation,
                                    WriteResponses::StreamDeleted.into(),
                                )?;

                                continue;
                            }

                            if let Some(e) =
                                optimistic_concurrency_check(expected, current_revision)
                            {
                                env.client.reply(
                                    mail.origin,
                                    mail.correlation,
                                    WriteResponses::WrongExpectedRevision {
                                        expected: e.expected,
                                        current: e.current,
                                    }
                                    .into(),
                                )?;

                                continue;
                            }

                            let revision = current_revision.next_revision();
                            let mut entries = ProposeEntries::new(ident, revision, events);
                            let receipt = log_writer.append(&mut entries)?;
                            env.handle.block_on(index_client.store(entries.indexes))?;

                            env.client.reply(
                                mail.origin,
                                mail.correlation,
                                WriteResponses::Committed {
                                    start_position: receipt.start_position,
                                    next_position: receipt.next_position,
                                    next_expected_version: ExpectedRevision::Revision(
                                        entries.revision,
                                    ),
                                }
                                .into(),
                            )?;

                            env.handle.block_on(sub_client.push(entries.committed))?;

                            continue;
                        }

                        WriteRequests::Delete { ident, expected } => {
                            let key = mikoshi_hash(&ident);
                            let current_revision =
                                env.handle.block_on(index_client.latest_revision(key))?;

                            if current_revision.is_deleted() {
                                env.client.reply(
                                    mail.origin,
                                    mail.correlation,
                                    WriteResponses::StreamDeleted.into(),
                                )?;

                                continue;
                            }

                            if let Some(e) =
                                optimistic_concurrency_check(expected, current_revision)
                            {
                                env.client.reply(
                                    mail.origin,
                                    mail.correlation,
                                    WriteResponses::WrongExpectedRevision {
                                        expected: e.expected,
                                        current: e.current,
                                    }
                                    .into(),
                                )?;

                                continue;
                            }

                            let revision = current_revision.next_revision();
                            let mut entries = ProposeEntries::new(
                                ident,
                                revision,
                                vec![Propose {
                                    id: Uuid::new_v4(),
                                    content_type: ContentType::Binary,
                                    class: STREAM_DELETED.to_string(),
                                    data: Bytes::default(),
                                }],
                            );
                            let receipt = log_writer.append(&mut entries)?;
                            env.handle.block_on(index_client.store(entries.indexes))?;

                            env.client.reply(
                                mail.origin,
                                mail.correlation,
                                WriteResponses::Committed {
                                    start_position: receipt.start_position,
                                    next_position: receipt.next_position,
                                    next_expected_version: ExpectedRevision::Revision(
                                        entries.revision,
                                    ),
                                }
                                .into(),
                            )?;

                            env.handle.block_on(sub_client.push(entries.committed))?;

                            continue;
                        }
                    }
                }
            }
        }
    }

    Ok(())
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
