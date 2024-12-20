use crate::process::indexing::IndexClient;
use crate::process::reading::{LogEntryExt, Request, Response};
use crate::process::{Item, ProcessRawEnv, RunnableRaw};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::chunks::ChunkContainer;
use geth_mikoshi::wal::LogReader;

pub struct Reading<S> {
    container: ChunkContainer<S>,
}

impl<S> Reading<S> {
    pub fn new(container: ChunkContainer<S>) -> Self {
        Self { container }
    }
}

impl<S> RunnableRaw for Reading<S>
where
    S: Storage,
{
    fn name(&self) -> &'static str {
        "reader"
    }

    fn run(self: Box<Self>, mut env: ProcessRawEnv) -> eyre::Result<()> {
        let batch_size = 500usize;
        let reader = LogReader::new(self.container);
        let mut index_client = IndexClient::resolve_raw(&mut env)?;

        while let Some(item) = env.queue.recv().ok() {
            match item {
                Item::Stream(stream) => {
                    if let Some(req) = Request::try_from(stream.payload) {
                        match req {
                            Request::Read {
                                ident,
                                start,
                                direction,
                                count,
                            } => {
                                let mut index_stream = env.handle.block_on(index_client.read(
                                    mikoshi_hash(ident),
                                    start.raw(),
                                    count,
                                    direction,
                                ))?;

                                if stream
                                    .sender
                                    .send(Response::Streaming.serialize(&mut env.buffer))
                                    .is_err()
                                {
                                    continue;
                                }

                                let mut count = 0usize;
                                while let Some((_, position)) =
                                    env.handle.block_on(index_stream.next())?
                                {
                                    reader.read_at(position)?.serialize(&mut env.buffer);
                                    count += 1;

                                    if count < batch_size {
                                        continue;
                                    }

                                    count = 0;
                                    if stream.sender.send(env.buffer.split().freeze()).is_err() {
                                        break;
                                    }
                                }

                                if !env.buffer.is_empty() {
                                    let _ = stream.sender.send(env.buffer.split().freeze());
                                }
                            }
                        }
                        continue;
                    }

                    tracing::warn!(
                        "malformed reader request from stream request {}",
                        stream.correlation
                    );
                }

                Item::Mail(mail) => {
                    tracing::warn!("mail {} ignored", mail.correlation);
                }
            }
        }

        Ok(())
    }
}
