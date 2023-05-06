mod backend;
mod constants;
pub mod hashing;
pub mod index;
pub mod storage;
pub mod wal;

use std::path::Path;

use crate::backend::Backend;
use bytes::Bytes;
use chrono::{DateTime, Utc};

use geth_common::{Direction, ExpectedRevision, Position, Propose, Record, Revision, WriteResult};
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Entry {
    pub id: Uuid,
    pub r#type: String,
    pub stream_name: String,
    pub revision: u64,
    pub data: Bytes,
    pub position: Position,
    pub created: DateTime<Utc>,
}

pub struct Mikoshi {
    backend: Box<dyn Backend + Send + 'static>,
    root: String,
}

impl Mikoshi {
    pub fn in_memory() -> Self {
        Self {
            backend: Box::new(backend::in_memory_backend()),
            root: "<in memory>".to_string(),
        }
    }

    pub fn esdb(path: impl AsRef<Path>) -> eyre::Result<Self> {
        let root = path.as_ref().to_string_lossy().to_string();
        Ok(Self {
            backend: Box::new(backend::esdb_backend(path)?),
            root,
        })
    }

    pub fn append(
        &mut self,
        stream_name: String,
        expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> eyre::Result<WriteResult> {
        self.backend.append(stream_name, expected, events)
    }

    pub fn read(
        &mut self,
        stream_name: String,
        starting: Revision<u64>,
        direction: Direction,
    ) -> eyre::Result<BoxedSyncMikoshiStream> {
        self.backend.read(stream_name, starting, direction)
    }

    pub fn root(&self) -> &str {
        self.root.as_str()
    }
}

pub type BoxedSyncMikoshiStream = Box<dyn SyncMikoshiStream + Sync + Send + 'static>;

pub trait SyncMikoshiStream {
    fn next(&mut self) -> eyre::Result<Option<Entry>>;
}

pub struct EmptyMikoshiStream;

impl SyncMikoshiStream for EmptyMikoshiStream {
    fn next(&mut self) -> eyre::Result<Option<Entry>> {
        Ok(None)
    }
}

pub struct MikoshiStream {
    inner: mpsc::Receiver<Entry>,
}

impl MikoshiStream {
    pub fn empty() -> Self {
        let (_, inner) = mpsc::channel(0);

        Self { inner }
    }

    pub fn new(inner: mpsc::Receiver<Entry>) -> Self {
        Self { inner }
    }

    pub fn from_vec(entries: Vec<Entry>) -> Self {
        let (sender, inner) = mpsc::channel(entries.len());

        tokio::spawn(async move {
            for entry in entries {
                let _ = sender.send(entry).await;
            }
        });

        Self { inner }
    }

    pub async fn next(&mut self) -> eyre::Result<Option<Record>> {
        if let Some(entry) = self.inner.recv().await {
            return Ok(Some(Record {
                id: entry.id,
                stream_name: entry.stream_name,
                position: entry.position,
                revision: entry.revision,
                data: entry.data,
                r#type: entry.r#type,
            }));
        }

        Ok(None)
    }
}
