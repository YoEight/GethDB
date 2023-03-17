use crate::backend::Backend;
use crate::{Entry, MikoshiStream};
use chrono::Utc;
use geth_common::{Direction, ExpectedRevision, Position, Propose, Revision, WriteResult};
use std::collections::HashMap;
use std::io;
use tokio::sync::mpsc;

#[derive(Default)]
pub struct InMemoryBackend {
    log: Vec<Entry>,
    indexes: HashMap<String, Vec<usize>>,
    revisions: HashMap<String, u64>,
}

impl Backend for InMemoryBackend {
    fn append(
        &mut self,
        stream_name: String,
        _expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> io::Result<WriteResult> {
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

        Ok(WriteResult {
            next_expected_version: ExpectedRevision::Revision(*rev),
            position: Position(log_position as u64),
        })
    }

    fn read(
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
    for (rev, idx) in indexes.into_iter().enumerate() {
        tracing::info!("Reading from index {}", idx);
        if let Revision::Revision(start) = starting {
            if (rev as u64) < start {
                continue;
            }
        }

        if let Some(entry) = log.get(idx).cloned() {
            let _ = sender.send(entry).await;
        } else {
            tracing::error!("Index {} is invalid", idx);
        }
    }
}
