use std::collections::HashMap;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use eyre::bail;
use geth_common::{Direction, ExpectedRevision, Position, Propose, Record, Revision};
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

#[derive(Default)]
pub struct Mikoshi {
    log: Vec<Entry>,
    indexes: HashMap<String, Vec<usize>>,
    revisions: HashMap<String, u64>,
}

impl Mikoshi {
    pub fn in_memory() -> Self {
        Default::default()
    }

    pub fn append(
        &mut self,
        stream_name: String,
        expected: ExpectedRevision,
        events: Vec<Propose>,
    ) {
        let mut log_position = self.log.len();
        let rev = self.revisions.entry(stream_name.clone()).or_default();
        let indexes = self.indexes.entry(stream_name.clone()).or_default();

        for event in events {
            self.log.push(Entry {
                id: event.id,
                stream_name: stream_name.clone(),
                revision: *rev,
                data: event.data,
                position: Position(log_position as u64),
                created: Utc::now(),
            });

            indexes.push(log_position);

            *rev += 1;
            log_position += 1;
        }
    }

    pub fn read(
        &self,
        stream_name: String,
        starting: Revision<u64>,
        direction: Direction,
    ) -> MikoshiStream {
        let (sender, inner) = mpsc::channel(500);
        let log = self.log.clone();
        let indexes = self
            .indexes
            .get(stream_name.as_str())
            .cloned()
            .unwrap_or_default();

        tokio::spawn(async move {
            match direction {
                Direction::Backward => {}
                Direction::Forward => read_forward(indexes, log, starting, sender).await,
            }
        });

        MikoshiStream { inner }
    }
}

async fn read_forward(
    indexes: Vec<usize>,
    log: Vec<Entry>,
    starting: Revision<u64>,
    sender: mpsc::Sender<Entry>,
) {
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
