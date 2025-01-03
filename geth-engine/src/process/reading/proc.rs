use std::cmp::min;
use std::mem;

use crate::process::indexing::IndexClient;
use crate::process::messages::{ReadRequests, ReadResponses};
use crate::process::{Item, ProcessRawEnv, Runtime};
use geth_common::ReadCompleted;
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::LogReader;

pub fn run<S>(runtime: Runtime<S>, mut env: ProcessRawEnv) -> eyre::Result<()>
where
    S: Storage + Send + Sync + 'static,
{
    let reader = LogReader::new(runtime.container().clone());
    let mut index_client = IndexClient::resolve_raw(&mut env)?;

    while let Some(item) = env.queue.recv().ok() {
        match item {
            Item::Stream(stream) => {
                if let Some(ReadRequests::Read {
                    ident,
                    start,
                    direction,
                    count,
                }) = stream.payload.try_into().ok()
                {
                    let index_stream = env.handle.block_on(index_client.read(
                        mikoshi_hash(ident),
                        start,
                        count,
                        direction,
                    ))?;

                    let mut index_stream = match index_stream {
                        ReadCompleted::Success(r) => r,
                        ReadCompleted::StreamDeleted => {
                            let _ = stream.sender.send(ReadResponses::StreamDeleted.into());

                            continue;
                        }
                    };

                    let batch_size = min(count, 500);
                    let mut batch = Vec::with_capacity(batch_size);
                    while let Some(entry) = env.handle.block_on(index_stream.next())? {
                        let entry = reader.read_at(entry.position)?;
                        batch.push(entry);

                        if batch.len() < batch_size {
                            continue;
                        }

                        let entries = mem::replace(&mut batch, Vec::with_capacity(batch_size));
                        if stream
                            .sender
                            .send(ReadResponses::Entries(entries).into())
                            .is_err()
                        {
                            break;
                        }
                    }

                    if !batch.is_empty() {
                        let _ = stream.sender.send(ReadResponses::Entries(batch).into());
                    }

                    continue;
                }

                tracing::warn!(
                    "malformed reader request from stream request {}",
                    stream.correlation
                );
            }

            Item::Mail(mail) => {
                if let Some(ReadRequests::ReadAt { position }) = mail.payload.try_into().ok() {
                    let entry = reader.read_at(position)?;
                    env.client.reply(
                        mail.origin,
                        mail.correlation,
                        ReadResponses::Entry(entry).into(),
                    )?;

                    continue;
                }

                tracing::warn!("mail {} ignored", mail.correlation);
            }
        }
    }

    Ok(())
}
