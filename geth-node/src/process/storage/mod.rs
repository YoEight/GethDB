mod service;

use eyre::bail;
use geth_mikoshi::index::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{ChunkManager, WriteAheadLog};
use tokio::sync::mpsc::{self, UnboundedReceiver};

use crate::bus::{AppendStreamMsg, ReadStreamMsg};
use crate::process::storage::service::{index, reader, writer};
use crate::process::subscriptions::SubscriptionsClient;

enum Msg {
    ReadStream(ReadStreamMsg),
    AppendStream(AppendStreamMsg),
}

pub struct StorageClient {
    inner: mpsc::UnboundedSender<Msg>,
}

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
}

pub fn start<WAL, S>(wal: WAL, index: Lsm<S>, sub_client: SubscriptionsClient) -> StorageClient
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let (sender, mailbox) = mpsc::unbounded_channel();
    let index_queue = index::start(manager.clone(), index.clone(), sub_client);
    let writer_queue = writer::start(wal, index.clone(), index_queue);
    let reader_queue = reader::start(manager.clone(), index.clone());

    tokio::spawn(service(mailbox, reader_queue, writer_queue));

    StorageClient { inner: sender }
}

async fn service(
    mut mailbox: UnboundedReceiver<Msg>,
    reader_queue: std::sync::mpsc::Sender<ReadStreamMsg>,
    writer_queue: std::sync::mpsc::Sender<AppendStreamMsg>,
) {
    while let Some(msg) = mailbox.recv().await {
        match msg {
            Msg::ReadStream(msg) => {
                if reader_queue.send(msg).is_err() {
                    tracing::error!("storage reader service is down");
                    break;
                }
            }

            Msg::AppendStream(msg) => {
                if writer_queue.send(msg).is_err() {
                    tracing::error!("storage writer service is down");
                    break;
                }
            }
        }
    }

    tracing::info!("storage service exited");
}
