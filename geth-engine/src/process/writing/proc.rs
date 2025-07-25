use crate::domain::index::CurrentRevision;
use crate::get_chunk_container;
use crate::metrics::get_metrics;
use crate::names::types::STREAM_DELETED;
use crate::process::messages::{WriteRequests, WriteResponses};
use crate::process::{Item, ProcessEnv, Raw};
use bytes::{Bytes, BytesMut};
use geth_common::{ContentType, ExpectedRevision, Propose, WrongExpectedRevisionError};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::wal::LogWriter;
use uuid::Uuid;

use super::entries::ProposeEntries;

pub fn run(mut env: ProcessEnv<Raw>) -> eyre::Result<()> {
    let mut log_writer = LogWriter::load(get_chunk_container(), BytesMut::with_capacity(4_096))?;
    let index_client = env.new_index_client()?;
    let sub_client = env.new_subscription_client()?;
    let metrics = get_metrics();

    while let Some(item) = env.recv() {
        match item {
            Item::Stream(_) => {
                continue;
            }

            Item::Mail(mail) => {
                if let Ok(req) = mail.payload.try_into() {
                    let (ident, expected, events) = match req {
                        WriteRequests::Write {
                            ident,
                            expected,
                            events,
                        } => (ident, expected, events),

                        WriteRequests::Delete { ident, expected } => {
                            tracing::debug!(
                                "received stream deletion request for stream {}",
                                ident
                            );

                            (
                                ident,
                                expected,
                                vec![Propose {
                                    id: Uuid::new_v4(),
                                    content_type: ContentType::Binary,
                                    class: STREAM_DELETED.to_string(),
                                    data: Bytes::default(),
                                }],
                            )
                        }
                    };

                    let key = mikoshi_hash(&ident);
                    let current_revision =
                        env.block_on(index_client.latest_revision(mail.context, key))?;

                    if current_revision.is_deleted() {
                        env.client.reply(
                            mail.context,
                            mail.origin,
                            mail.correlation,
                            WriteResponses::StreamDeleted.into(),
                        )?;

                        continue;
                    }

                    if let Some(e) = optimistic_concurrency_check(expected, current_revision) {
                        env.client.reply(
                            mail.context,
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
                    let mut entries = ProposeEntries::new(metrics.clone(), ident, revision, events);
                    let span = tracing::info_span!("append_entries_to_log", correlation = %mail.context.correlation);

                    match span.in_scope(|| log_writer.append(&mut entries)) {
                        Err(e) => {
                            tracing::error!("error when appending to stream: {}", e);
                            metrics.observe_write_error();

                            env.client.reply(
                                mail.context,
                                mail.origin,
                                mail.correlation,
                                WriteResponses::Error.into(),
                            )?;
                        }

                        Ok(receipt) => {
                            env.block_on(index_client.store(mail.context, entries.indexes))?;

                            env.client.reply(
                                mail.context,
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

                            env.block_on(sub_client.push(mail.context, entries.committed))?;
                        }
                    }

                    continue;
                }

                tracing::warn!(correlation = %mail.correlation, "request was not handled");
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
