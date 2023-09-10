use crate::process::storage::service::current::CurrentRevision;
use crate::process::storage::RevisionCache;
use crate::process::subscriptions::SubscriptionsClient;
use chrono::{TimeZone, Utc};
use geth_common::Position;
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::index::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::records::Records;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};
use geth_mikoshi::Entry;
use geth_mikoshi::IteratorIO;
use std::io;

#[derive(Clone)]
pub struct StorageIndex<S>
where
    S: Storage,
{
    index: Lsm<S>,
    revision_cache: RevisionCache,
    chase_sender: std::sync::mpsc::Sender<u64>,
}

impl<S> StorageIndex<S>
where
    S: Storage + Send + Sync + 'static,
{
    pub fn new<WAL>(
        wal: WALRef<WAL>,
        index: Lsm<S>,
        subscriptions: SubscriptionsClient,
        revision_cache: RevisionCache,
    ) -> Self
    where
        WAL: WriteAheadLog + Send + Sync + 'static,
    {
        let (chase_sender, recv) = std::sync::mpsc::channel();
        let chase_index = index.clone();
        let starting_position = wal.write_position();

        // Starting index chase process.
        std::thread::spawn(move || {
            let mut current = starting_position;
            while let Ok(next) = recv.recv() {
                tracing::debug!("Request to chase WAL from {}", next);
                if current > next {
                    tracing::debug!(
                        "Discard chase request because current index checkpoint ({}) is greater to requested starting point ({})",
                        current,
                        next,
                    );
                    continue;
                }

                let records = wal.records(next).map(|(position, record)| match record {
                    Records::StreamEventAppended(record) => {
                        let key = mikoshi_hash(&record.event_stream_id);

                        let _ = subscriptions.event_committed(Entry {
                            id: record.event_id,
                            r#type: record.event_type,
                            stream_name: record.event_stream_id,
                            revision: record.revision,
                            data: record.data,
                            position: Position(position),
                            created: Utc.timestamp_opt(record.created, 0).unwrap(),
                        });

                        (key, record.revision, position)
                    }

                    Records::StreamDeleted(record) => {
                        let key = mikoshi_hash(&record.event_stream_id);

                        (key, record.revision, u64::MAX)
                    }
                });

                if let Err(e) = chase_index.put(records) {
                    tracing::error!("Error when indexing entries from position {}: {}", next, e);
                } else {
                    tracing::debug!("Chasing completed {} -> {}", current, next);
                    current = next;
                }
            }
        });

        Self {
            index,
            chase_sender,
            revision_cache,
        }
    }

    pub fn lsm(&self) -> &Lsm<S> {
        &self.index
    }

    pub fn stream_current_revision(&self, stream_name: &str) -> io::Result<CurrentRevision> {
        let stream_key = mikoshi_hash(stream_name);
        let current_revision = if let Some(current) = self.revision_cache.get(stream_name) {
            CurrentRevision::Revision(current)
        } else {
            let revision = self
                .index
                .highest_revision(stream_key)?
                .map_or_else(|| CurrentRevision::NoStream, CurrentRevision::Revision);

            if let CurrentRevision::Revision(rev) = revision {
                self.revision_cache.insert(stream_name.to_string(), rev);
            }

            revision
        };

        Ok(current_revision)
    }

    pub fn chase(&self, from_position: u64) {
        let _ = self.chase_sender.send(from_position);
    }
}
