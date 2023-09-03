pub mod reader;
mod service;
mod writer;

use eyre::bail;
use geth_mikoshi::index::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};
use tokio::sync::mpsc::{self, UnboundedReceiver};

use crate::bus::{AppendStreamMsg, DeleteStreamMsg, ReadStreamMsg};
use crate::process::storage::reader::StorageReader;
use crate::process::storage::writer::StorageWriter;
use crate::process::subscriptions::SubscriptionsClient;

enum Msg {
    ReadStream(ReadStreamMsg),
    AppendStream(AppendStreamMsg),
    DeleteStream(DeleteStreamMsg),
}

pub struct StorageRef {
    inner: mpsc::UnboundedSender<Msg>,
}

pub type RevisionCache = moka::sync::Cache<String, u64>;

impl StorageRef {
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

pub fn start<WAL, S>(wal: WALRef<WAL>, index: Lsm<S>, sub_client: SubscriptionsClient) -> StorageRef
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let revision_cache = moka::sync::Cache::<String, u64>::builder()
        .max_capacity(10_000)
        .name("revision-cache")
        .build();

    let writer = StorageWriter::new(wal.clone(), index.clone(), revision_cache.clone());
    let reader = StorageReader::new(wal.clone(), index.clone(), revision_cache.clone());

    let (sender, mailbox) = mpsc::unbounded_channel();

    tokio::spawn(service(mailbox, reader, writer));

    StorageRef { inner: sender }
}

async fn service<WAL, S>(
    mut mailbox: UnboundedReceiver<Msg>,
    reader: StorageReader<WAL, S>,
    writer: StorageWriter<WAL, S>,
) where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    while let Some(msg) = mailbox.recv().await {
        match msg {
            Msg::ReadStream(msg) => {
                let _ = msg.mail.send(Ok(reader.read(msg.payload).await));
            }

            Msg::AppendStream(msg) => {
                let _ = msg.mail.send(Ok(writer.append(msg.payload).await));
            }

            Msg::DeleteStream(msg) => {
                let _ = msg.mail.send(Ok(writer.delete(msg.payload).await));
            }
        }
    }

    tracing::info!("storage service exited");
}
