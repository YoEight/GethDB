use std::io;

use bytes::{Buf, Bytes};
use futures::TryStreamExt;

use geth_common::{Client, Revision, SubscriptionEvent};
use geth_domain::Lsm;
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::storage::{FileId, Storage};

use crate::names;

pub async fn indexing<C, S>(client: C, lsm: Lsm<S>) -> eyre::Result<()>
where
    C: Client,
    S: Storage + Send + Sync + 'static,
{
    let storage = lsm.storage().clone();
    let globals_hash = mikoshi_hash(names::streams::GLOBALS);
    let index_chk = FileId::index_chk();
    let index_global_chk = FileId::index_global_chk();
    let mut index_pos = 0u64;
    let mut index_global_pos = 0u64;

    if storage.exists(index_chk)? {
        index_pos = storage.read_from(index_chk, 0, 8)?.get_u64_le();
    } else {
        flush_chk(&storage, index_chk, 0)?;
    }

    if storage.exists(index_global_chk)? {
        index_global_pos = storage.read_from(index_global_chk, 0, 8)?.get_u64_le();
    } else {
        flush_chk(&storage, index_global_chk, 0)?;
    }

    let starting = if index_pos == 0 {
        Revision::Start
    } else {
        Revision::Revision(index_pos)
    };

    let mut stream = client
        .subscribe_to_stream(names::streams::ALL, starting)
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

            SubscriptionEvent::EventAppeared(event) => {
                let is_deleted = event.r#type == names::types::STREAM_DELETED;
                let position = event.position.raw();
                let global = index_global_pos;
                let stream_hash = mikoshi_hash(&event.stream_name);
                let revision = event.revision;

                if is_deleted {
                    lsm.put_single(stream_hash, u64::MAX, position)?;
                } else {
                    lsm.put_values([
                        (stream_hash, revision, position),
                        (globals_hash, global, position),
                    ])?;

                    index_global_pos += 1;
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
