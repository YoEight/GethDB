use crate::domain::index::CurrentRevision;
use crate::process::indexing::IndexClient;
use crate::process::messages::{WriteRequests, WriteResponses};
use crate::process::subscription::SubscriptionClient;
use crate::process::{Item, ProcessRawEnv, Runtime};
use geth_common::{ExpectedRevision, WrongExpectedRevisionError};
use geth_domain::index::BlockEntry;
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{LogEntries, LogWriter};

pub fn run<S>(runtime: Runtime<S>, env: ProcessRawEnv) -> eyre::Result<()>
where
    S: Storage + 'static,
{
    let pool = env.client.pool.clone();
    let mut buffer = env.handle.block_on(pool.get()).unwrap();
    let mut log_writer = LogWriter::load(runtime.container().clone(), buffer.split())?;
    let mut index_client = IndexClient::resolve_raw(&env)?;
    let sub_client = SubscriptionClient::resolve_raw(&env)?;
    let mut entries = LogEntries::new();
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
                        WriteRequests::GetWritePosition => {
                            env.client.reply(
                                mail.origin,
                                mail.correlation,
                                WriteResponses::WritePosition(log_writer.writer_position()).into(),
                            )?;
                        }

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
                            let count = events.len() as u64;
                            entries.begin(ident, revision, events);
                            let receipt = log_writer.append(&mut entries)?;
                            let index_entries = entries
                                .complete()
                                .map(|(k, r, p)| BlockEntry {
                                    key: k,
                                    revision: r,
                                    position: p,
                                })
                                .collect();

                            env.handle.block_on(index_client.store(index_entries))?;

                            env.client.reply(
                                mail.origin,
                                mail.correlation,
                                WriteResponses::Committed {
                                    start_position: receipt.start_position,
                                    next_position: receipt.next_position,
                                    next_expected_version: ExpectedRevision::Revision(
                                        revision + count,
                                    ),
                                }
                                .into(),
                            )?;

                            let log_entries = entries.committed_events();
                            env.handle.block_on(sub_client.push(log_entries))?;

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
