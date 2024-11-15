use std::io;

use geth_common::{AppendStreamCompleted, DeleteStreamCompleted};
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};

use crate::domain::index::IndexRef;
use crate::messages::{AppendStream, DeleteStream, ReadStream, ReadStreamCompleted};
use crate::process::storage::reader::StorageReader;
use crate::process::storage::writer::{new_storage_writer, StorageWriter};
use crate::process::subscriptions::SubscriptionsClient;

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

impl<WAL, S> StorageService<WAL, S>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    pub fn new(wal: WALRef<WAL>, index: IndexRef<S>, sub_client: SubscriptionsClient) -> Self {
        Self {
            writer: new_storage_writer(wal.clone(), index.clone(), sub_client),
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
