use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use uuid::Uuid;

use geth_common::{Position, Record};

pub use crate::storage::fs::FileSystemStorage;
pub use crate::storage::in_mem::InMemoryStorage;

mod constants;
pub mod hashing;
pub mod storage;
pub mod wal;

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
    inner: mpsc::UnboundedReceiver<Entry>,
}

impl MikoshiStream {
    pub fn empty() -> Self {
        let (_, inner) = mpsc::unbounded_channel();

        Self { inner }
    }

    pub fn new(inner: mpsc::UnboundedReceiver<Entry>) -> Self {
        Self { inner }
    }

    pub fn from_vec(entries: Vec<Entry>) -> Self {
        let (sender, inner) = mpsc::unbounded_channel();

        for entry in entries {
            let _ = sender.send(entry);
        }

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
