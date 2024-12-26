use crate::domain::index::CurrentRevision;
use crate::process::indexing::IndexClient;
use crate::process::subscription::SubscriptionClient;
use crate::process::writing::{Request, Response};
use crate::process::{subscription, Item, ProcessRawEnv, Runtime};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use geth_common::{ExpectedRevision, WrongExpectedRevisionError};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::storage::{FileId, Storage};
use geth_mikoshi::wal::chunks::ChunkContainer;
use geth_mikoshi::wal::{LogEntries, LogWriter, WriteAheadLog};
use std::io;
use uuid::Uuid;

pub struct Writing;

pub fn run<S>(runtime: Runtime<S>, mut env: ProcessRawEnv) -> eyre::Result<()>
where
    S: Storage + 'static,
{
    let mut log_writer = LogWriter::load(runtime.container().clone(), env.buffer.split())?;
    let mut index_client = IndexClient::resolve_raw(&mut env)?;
    let mut sub_client = SubscriptionClient::resolve_raw(&mut env)?;
    let mut entries = LogEntries::new(env.buffer.split());

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
                    events,
                    ..
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
                    entries.begin(ident, revision, events);
                    let receipt = log_writer.append(&mut entries)?;
                    let index_entries = entries.complete();

                    env.handle.block_on(index_client.store_raw(index_entries))?;

                    env.client.reply(
                        mail.origin,
                        mail.correlation,
                        Response::committed(receipt.start_position, receipt.next_position)
                            .serialize(&mut env.buffer),
                    )?;

                    let mut builder = subscription::Request::push(&mut env.buffer);
                    for event in entries.committed_events() {
                        builder.push_entry(event);
                    }

                    sub_client.push(builder)?;

                    continue;
                }

                tracing::warn!("unhandled mail request {}", mail.correlation);
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
