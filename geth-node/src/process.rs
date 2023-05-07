use crate::bus::{Mailbox, Msg};
use geth_mikoshi::index::Lsm;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::ChunkManager;

mod storage;

pub fn start<S>(mut mailbox: Mailbox, manager: ChunkManager<S>, index: Lsm<S>)
where
    S: Storage + Send + Sync + 'static,
{
    let storage_client = storage::start(manager, index);

    tokio::spawn(async move {
        while let Some(msg) = mailbox.next().await {
            match msg {
                Msg::AppendStream(msg) => {
                    storage_client.append_stream(msg).await?;
                }

                Msg::ReadStream(msg) => {
                    storage_client.read_stream(msg).await?;
                }
            }
        }

        Ok::<_, eyre::Report>(())
    });
}
