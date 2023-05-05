use eyre::bail;
use geth_mikoshi::index::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::ChunkManager;
use geth_mikoshi::{Mikoshi, MikoshiStream};
use tokio::sync::{
    mpsc::{self, UnboundedReceiver},
    oneshot,
};

use crate::messages::{AppendStream, AppendStreamCompleted, ReadStream, ReadStreamCompleted};
enum Msg {
    ReadStream(ReadStream, oneshot::Sender<ReadStreamCompleted>),
    AppendStream(AppendStream, oneshot::Sender<AppendStreamCompleted>),
}

pub struct StorageClient {
    inner: mpsc::UnboundedSender<Msg>,
}

impl StorageClient {
    pub async fn read_stream(
        &self,
        msg: ReadStream,
        callback: oneshot::Sender<ReadStreamCompleted>,
    ) -> eyre::Result<()> {
        if self.inner.send(Msg::ReadStream(msg, callback)).is_err() {
            bail!("Main bus has shutdown!");
        }

        Ok(())
    }

    pub async fn append_stream(
        &self,
        msg: AppendStream,
        callback: oneshot::Sender<AppendStreamCompleted>,
    ) -> eyre::Result<()> {
        if self.inner.send(Msg::AppendStream(msg, callback)).is_err() {
            bail!("Main bus has shutdown!");
        }

        Ok(())
    }
}

pub fn start<S>(manager: ChunkManager<S>, index: Lsm<S>) -> StorageClient
where
    S: Storage + Send + Sync + 'static,
{
    let (sender, mailbox) = mpsc::unbounded_channel();

    tokio::spawn(service(mailbox, manager, index));

    StorageClient { inner: sender }
}

async fn service<S>(mut mailbox: UnboundedReceiver<Msg>, manager: ChunkManager<S>, index: Lsm<S>)
where
    S: Storage + Send + 'static,
{
    let mut mikoshi = Mikoshi::in_memory();

    while let Some(msg) = mailbox.recv().await {
        match msg {
            Msg::ReadStream(params, callback) => {
                let (sender, inner) = mpsc::channel(500);
                let mut stream = MikoshiStream::new(inner);
                let mut reader = mikoshi
                    .read(params.stream_name, params.starting, params.direction)
                    .expect("to handle error properly");

                // TODO - We will move to proper async I/O in the long run.
                tokio::task::spawn_blocking(move || {
                    while let Some(entry) = reader.next()? {
                        if sender.blocking_send(entry).is_err() {
                            break;
                        }
                    }

                    Ok::<_, eyre::Report>(())
                });

                let _ = callback.send(ReadStreamCompleted {
                    correlation: params.correlation,
                    reader: stream,
                });
            }

            Msg::AppendStream(params, callback) => {
                let result = mikoshi
                    .append(params.stream_name, params.expected, params.events)
                    .expect("to handle error properly");

                let _ = callback.send(AppendStreamCompleted {
                    correlation: params.correlation,
                    result,
                });
            }
        }
    }
}
