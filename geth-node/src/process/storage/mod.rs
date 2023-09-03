pub mod reader;
mod service;
mod writer;

use geth_mikoshi::index::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};

use crate::messages::{
    AppendStream, AppendStreamCompleted, DeleteStream, DeleteStreamCompleted, ReadStream,
    ReadStreamCompleted,
};
use crate::process::storage::reader::StorageReader;
use crate::process::storage::writer::StorageWriter;
use crate::process::subscriptions::SubscriptionsClient;

#[derive(Clone)]
pub struct StorageService<WAL, S> {
    writer: StorageWriter<WAL, S>,
    reader: StorageReader<WAL, S>,
    client: SubscriptionsClient,
}

pub type RevisionCache = moka::sync::Cache<String, u64>;

impl<WAL, S> StorageService<WAL, S>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    pub fn new(wal: WALRef<WAL>, index: Lsm<S>, client: SubscriptionsClient) -> Self {
        let revision_cache = moka::sync::Cache::<String, u64>::builder()
            .max_capacity(10_000)
            .name("revision-cache")
            .build();

        Self {
            writer: StorageWriter::new(wal.clone(), index.clone(), revision_cache.clone()),
            reader: StorageReader::new(wal, index, revision_cache),
            client,
        }
    }

    pub async fn read_stream(&self, params: ReadStream) -> ReadStreamCompleted {
        self.reader.read(params).await
    }

    pub async fn append_stream(&self, params: AppendStream) -> AppendStreamCompleted {
        self.writer.append(params).await
    }

    pub async fn delete_stream(&self, params: DeleteStream) -> DeleteStreamCompleted {
        self.writer.delete(params).await
    }
}
