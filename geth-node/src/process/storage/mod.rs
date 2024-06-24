use std::io;

use geth_common::{AppendStreamCompleted, DeleteStreamCompleted};
use geth_domain::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};

use crate::messages::{AppendStream, DeleteStream, ReadStream, ReadStreamCompleted};
use crate::process::storage::index::StorageIndex;
use crate::process::storage::reader::StorageReader;
use crate::process::storage::writer::{new_storage_writer, StorageWriter};
use crate::process::subscriptions::SubscriptionsClient;

pub mod index;
pub mod reader;
mod writer;

#[derive(Clone)]
pub struct StorageService<WAL, S>
where
    S: Storage,
{
    writer: StorageWriter,
    reader: StorageReader<WAL, S>,
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

        let index = StorageIndex::new(wal.clone(), index.clone(), client, revision_cache.clone());

        Self {
            writer: new_storage_writer(wal.clone(), index.clone(), revision_cache.clone()),
            reader: StorageReader::new(wal, index),
        }
    }

    pub async fn read_stream(&self, params: ReadStream) -> ReadStreamCompleted {
        self.reader.read(params).await
    }

    pub async fn append_stream(&self, params: AppendStream) -> io::Result<AppendStreamCompleted> {
        let writer = self.writer.clone();
        match tokio::task::spawn_blocking(move || writer.append(params)).await {
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
            Ok(r) => r,
        }
    }

    pub async fn delete_stream(&self, params: DeleteStream) -> io::Result<DeleteStreamCompleted> {
        let writer = self.writer.clone();
        match tokio::task::spawn_blocking(move || writer.delete(params)).await {
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
            Ok(r) => r,
        }
    }
}
