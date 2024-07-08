use std::io;

use bytes::{Buf, Bytes};
use futures::TryStreamExt;

use geth_common::{Client, Direction, Position, Record, Revision, SubscriptionEvent};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::storage::{FileId, Storage};

use crate::domain::index::Index;
use crate::names;
use crate::process::SubscriptionsClient;

pub async fn indexing<C, S>(
    client: C,
    index: Index<S>,
    sub_client: SubscriptionsClient,
) -> eyre::Result<()>
where
    C: Client,
    S: Storage + Send + Sync + 'static,
{
    let mut internal = Internal::new(index, sub_client.clone())?;

    let starting = if internal.index_pos == 0 {
        Revision::Start
    } else {
        Revision::Revision(internal.index_pos)
    };

    let mut stream = client
        .subscribe_to_stream(names::streams::SYSTEM, starting)
        .await;

    while let Some(event) = stream.try_next().await? {
        match event {
            SubscriptionEvent::Unsubscribed(_) => {
                tracing::warn!("indexing service subscription to $all unsubscribed. exiting...");
                break;
            }

            SubscriptionEvent::Confirmed(_) => {
                tracing::info!("indexing service subscription to $all is live");
            }

            // OPTIMIZATION - When rebuilding the index from scratch, we could group events together
            // to decrease sys-calls when filling up the index.
            SubscriptionEvent::CaughtUp => {
                tracing::info!(
                    "indexing service has caught up with not indexed data, switch to live mode"
                );
            }

            SubscriptionEvent::EventAppeared(record) => {
                if record.stream_name != names::streams::SYSTEM {
                    internal.index_record(record)?;
                    continue;
                }

                if record.r#type == names::types::EVENTS_WRITTEN {
                    let mut stream = client
                        .read_stream(
                            names::streams::ALL,
                            Direction::Forward,
                            Revision::Revision(record.position.raw()),
                            u64::MAX,
                        )
                        .await;

                    let mut position = 0;
                    while let Some(record) = stream.try_next().await? {
                        position = record.position.raw();
                        internal.index_record(record)?;
                    }

                    sub_client.event_committed(Record {
                        id: Default::default(),
                        r#type: names::types::EVENTS_INDEXED.to_string(),
                        stream_name: names::streams::SYSTEM.to_string(),
                        position: Position(position),
                        revision: position,
                        data: Default::default(),
                    })?
                }
            }
        }
    }

    Ok(())
}

fn flush_chk<S: Storage>(storage: &S, file_id: FileId, pos: u64) -> io::Result<()> {
    storage.write_to(
        file_id,
        0,
        Bytes::copy_from_slice(pos.to_le_bytes().as_slice()),
    )
}

const INDEX_CHK: FileId = FileId::index_chk();
const INDEX_GLOBAL_CHK: FileId = FileId::index_global_chk();

struct Internal<S> {
    index: Index<S>,
    sub_client: SubscriptionsClient,
    global_hash: u64,
    index_pos: u64,
    index_global_pos: u64,
}

impl<S> Internal<S>
where
    S: Storage + Send + Sync + 'static,
{
    fn new(index: Index<S>, sub_client: SubscriptionsClient) -> eyre::Result<Self> {
        let mut index_pos = 0u64;
        let mut index_global_pos = 0u64;

        if index.storage().exists(INDEX_CHK)? {
            index_pos = index.storage().read_from(INDEX_CHK, 0, 8)?.get_u64_le();
        } else {
            flush_chk(index.storage(), INDEX_CHK, 0)?;
        }

        if index.storage().exists(INDEX_GLOBAL_CHK)? {
            index_global_pos = index
                .storage()
                .read_from(INDEX_GLOBAL_CHK, 0, 8)?
                .get_u64_le();
        } else {
            flush_chk(index.storage(), INDEX_GLOBAL_CHK, 0)?;
        }

        Ok(Self {
            index,
            sub_client,
            index_pos,
            index_global_pos,
            global_hash: mikoshi_hash(names::streams::GLOBALS),
        })
    }

    fn index_record(&mut self, record: Record) -> eyre::Result<()> {
        let is_deleted = record.r#type == names::types::STREAM_DELETED;
        let position = record.position.raw();
        let global = self.index_global_pos;
        let stream_hash = mikoshi_hash(&record.stream_name);
        let revision = record.revision;

        if is_deleted {
            self.index.register(stream_hash, u64::MAX, position)?;
        } else {
            self.index.register_multiple([
                (stream_hash, revision, position),
                (self.global_hash, global, position),
            ])?;

            self.index_global_pos += 1;

            self.sub_client.event_committed(record)?;
        }

        Ok(())
    }
}
