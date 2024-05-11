use std::io;

use geth_common::{ExpectedRevision, Position};
use geth_domain::binary::events::Event;
use geth_domain::parse_event;
use geth_mikoshi::{Entry, IteratorIO};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::index::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};

use crate::process::storage::RevisionCache;
use crate::process::subscriptions::SubscriptionsClient;

#[derive(Copy, Clone)]
pub enum CurrentRevision {
    NoStream,
    Revision(u64),
}

impl CurrentRevision {
    pub fn next_revision(self) -> u64 {
        match self {
            CurrentRevision::NoStream => 0,
            CurrentRevision::Revision(r) => r + 1,
        }
    }

    pub fn as_expected(self) -> ExpectedRevision {
        match self {
            CurrentRevision::NoStream => ExpectedRevision::NoStream,
            CurrentRevision::Revision(v) => ExpectedRevision::Revision(v),
        }
    }

    pub fn is_deleted(&self) -> bool {
        if let CurrentRevision::Revision(r) = self {
            return *r == u64::MAX;
        }

        false
    }
}

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

                let records = wal.entries(next).map_io(|entry| {
                    let event = parse_event(&entry.payload)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

                    match event.event.unwrap() {
                        Event::RecordedEvent(event) => {
                            let key = mikoshi_hash(&event.stream_name);
                            let event = geth_domain::RecordedEvent::from(event);
                            let revision = event.revision;
                            let _ = subscriptions.event_committed(Entry {
                                id: event.id,
                                r#type: event.class,
                                stream_name: event.stream_name,
                                revision: event.revision,
                                data: event.data.into(),
                                position: Position(entry.position),
                                created: event.created,
                            });

                            Ok((key, revision, entry.position))
                        }

                        Event::StreamDeleted(event) => {
                            let key = mikoshi_hash(&event.stream_name);

                            Ok((key, event.revision, u64::MAX))
                        }
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

// TODO - We need to move the index out of mikoshi because after thinking about it, indexing is a
// projection of the transaction log, not something related it. For example, the index can be
// rebuilt at any given time.
pub fn rebuild_index<S, WAL>(lsm: &Lsm<S>, wal: &WALRef<WAL>) -> io::Result<()>
where
    S: Storage + Send + Sync + 'static,
    WAL: WriteAheadLog + Send,
{
    let logical_position = lsm.checkpoint();
    let entries = wal
        .entries(logical_position)
        .map_io(|entry| match parse_event(&entry.payload) {
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),

            Ok(evt) => match evt.event.unwrap() {
                Event::RecordedEvent(event) => {
                    let key = mikoshi_hash(&event.stream_name);
                    Ok((key, event.revision, entry.position))
                }

                Event::StreamDeleted(event) => {
                    let key = mikoshi_hash(&event.stream_name);

                    Ok((key, u64::MAX, entry.position))
                }
            },
        });

    lsm.put(entries)?;
    let writer_checkpoint = wal.write_position();
    lsm.set_checkpoint(writer_checkpoint);

    Ok(())
}
