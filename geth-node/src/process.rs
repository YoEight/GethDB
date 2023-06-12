use crate::bus::{Mailbox, Msg};
use geth_mikoshi::index::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::ChunkManager;

mod storage;
mod subscriptions;

pub fn start<S>(mut mailbox: Mailbox, manager: ChunkManager<S>, index: Lsm<S>)
where
    S: Storage + Send + Sync + 'static,
{
    let storage = storage::start(manager, index);
    let subscriptions = subscriptions::start();

    tokio::spawn(async move {
        while let Some(msg) = mailbox.next().await {
            match msg {
                Msg::AppendStream(msg) => {
                    storage.append_stream(msg).await?;
                }

                Msg::ReadStream(msg) => {
                    storage.read_stream(msg).await?;
                }

                Msg::Subscribe(msg) => {
                    subscriptions.subscribe(msg).await?;
                }
            }
        }

        Ok::<_, eyre::Report>(())
    });
}
