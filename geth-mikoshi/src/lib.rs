mod constants;
pub mod hashing;
pub mod index;
pub mod storage;
pub mod wal;

use std::path::Path;

use bytes::Bytes;
use chrono::{DateTime, Utc};

use geth_common::{Direction, ExpectedRevision, Position, Propose, Record, Revision, WriteResult};
use tokio::sync::mpsc;
use uuid::Uuid;

pub use index::IteratorIO;

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
