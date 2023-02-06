use std::collections::HashMap;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use eyre::bail;
use geth_common::{Position, Record};
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Debug)]
struct Entry {
    pub id: Uuid,
    pub stream_name: String,
    pub revision: u64,
    pub data: Bytes,
    pub position: Position,
    pub created: DateTime<Utc>,
}

#[derive(Default)]
pub struct Mikoshi {
    log: Vec<Entry>,
    indexes: HashMap<String, usize>,
    revisions: HashMap<String, u64>,
    log_position: u64,
}

impl Mikoshi {
    pub fn in_memory() -> Self {
        Default::default()
    }
}

pub struct MikoshiStream {
    inner: mpsc::Receiver<Entry>,
}

impl MikoshiStream {
    pub async fn next(&mut self) -> eyre::Result<Option<Record>> {
        if let Some(entry) = self.inner.recv().await {
            return Ok(Some(Record {
                id: entry.id,
                stream_name: entry.stream_name,
                position: entry.position,
                revision: entry.revision,
                data: entry.data,
            }));
        }

        Ok(None)
    }
}
