mod backend;

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
        // TODO - Implement better error handling.
        self.backend.append(stream_name, expected, events).unwrap()
    }

    pub fn read(
        &mut self,
        stream_name: String,
        starting: Revision<u64>,
        direction: Direction,
    ) -> MikoshiStream {
        self.backend.read(stream_name, starting, direction).unwrap()
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

    pub fn from_vec(entries: Vec<Entry>) -> Self {
        let (mut sender, inner) = mpsc::channel(entries.len());

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
            }));
        }

        Ok(None)
    }
}
