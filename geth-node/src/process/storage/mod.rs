mod service;
mod writer;

use eyre::bail;
use geth_mikoshi::index::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};
use tokio::sync::mpsc::{self, UnboundedReceiver};

use crate::bus::{AppendStreamMsg, DeleteStreamMsg, ReadStreamMsg};
use crate::process::storage::service::{index, reader};
use crate::process::storage::writer::StorageWriter;
use crate::process::subscriptions::SubscriptionsClient;

enum Msg {
    ReadStream(ReadStreamMsg),
    AppendStream(AppendStreamMsg),
    DeleteStream(DeleteStreamMsg),
}

pub struct StorageClient {
    inner: mpsc::UnboundedSender<Msg>,
}

pub type RevisionCache = moka::sync::Cache<String, u64>;

impl StorageClient {
    pub async fn read_stream(&self, msg: ReadStreamMsg) -> eyre::Result<()> {
        if self.inner.send(Msg::ReadStream(msg)).is_err() {
            bail!("Main bus has shutdown!");
        }

        Ok(())
    }

    pub async fn append_stream(&self, msg: AppendStreamMsg) -> eyre::Result<()> {
        if self.inner.send(Msg::AppendStream(msg)).is_err() {
            bail!("Main bus has shutdown!");
        }

        Ok(())
    }

    pub async fn delete_stream(&self, msg: DeleteStreamMsg) -> eyre::Result<()> {
        if self.inner.send(Msg::DeleteStream(msg)).is_err() {
            bail!("Main bus has shutdown!");
        }

        Ok(())
    }
}

pub fn start<WAL, S>(
    wal: WALRef<WAL>,
    index: Lsm<S>,
    sub_client: SubscriptionsClient,
) -> StorageClient
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let revision_cache = moka::sync::Cache::<String, u64>::builder()
        .max_capacity(10_000)
        .name("revision-cache")
        .build();

    let writer = StorageWriter::new(wal.clone(), index.clone(), revision_cache.clone());

    let (sender, mailbox) = mpsc::unbounded_channel();
    let index_queue = index::start(wal.clone(), index.clone(), sub_client);
    let reader_queue = reader::start(wal, index.clone(), revision_cache);

    tokio::spawn(service(mailbox, reader_queue, writer));

    StorageClient { inner: sender }
}

async fn service<WAL, S>(
    mut mailbox: UnboundedReceiver<Msg>,
    reader_queue: std::sync::mpsc::Sender<ReadStreamMsg>,
    mut writer: StorageWriter<WAL, S>,
) where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    while let Some(msg) = mailbox.recv().await {
        match msg {
            Msg::ReadStream(msg) => {
                if reader_queue.send(msg).is_err() {
                    tracing::error!("storage reader service is down");
                    break;
                }
            }

            Msg::AppendStream(msg) => {
                let stream_name = msg.payload.stream_name.clone();
                let result = match writer.append(msg.payload) {
                    Err(e) => {
                        tracing::error!("Error when appending stream '{}': {}", stream_name, e);
                        Err(e.into())
                    }

                    Ok(result) => Ok(result),
                };

                let _ = msg.mail.send(result);
            }

            Msg::DeleteStream(msg) => {
                let stream_name = msg.payload.stream_name.clone();
                let result = match writer.delete(msg.payload) {
                    Err(e) => {
                        tracing::error!("Error when deleting stream '{}': {}", stream_name, e);
                        Err(e.into())
                    }

                    Ok(result) => Ok(result),
                };

                let _ = msg.mail.send(result);
            }
        }
    }

    tracing::info!("storage service exited");
}
