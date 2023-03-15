mod backend;
mod manager;
mod parsing;
mod types;
mod utils;

use std::collections::HashMap;

use crate::backend::Backend;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use eyre::bail;
use geth_common::{Direction, ExpectedRevision, Position, Propose, Record, Revision, WriteResult};
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct Entry {
    pub id: Uuid,
    pub stream_name: String,
    pub revision: u64,
    pub data: Bytes,
    pub position: Position,
    pub created: DateTime<Utc>,
}

pub struct Mikoshi {
    backend: Box<dyn Backend + Send + 'static>,
}

impl Mikoshi {
    pub fn in_memory() -> Self {
        Self {
            backend: Box::new(backend::in_memory_backend()),
        }
    }

    pub fn append(
        &mut self,
        stream_name: String,
        expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> WriteResult {
        self.backend.append(stream_name, expected, events)
    }

    pub fn read(
        &self,
        stream_name: String,
        starting: Revision<u64>,
        direction: Direction,
    ) -> MikoshiStream {
        self.backend.read(stream_name, starting, direction)
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
