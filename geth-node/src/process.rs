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
    let subscriptions = subscriptions::start();
    let storage = storage::start(manager, index, subscriptions.clone());

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
                    if subscriptions.subscribe(msg).is_err() {
                        tracing::warn!("Subscriptions service is not longer available. quitting");
                        break;
                    }
                }

                Msg::GetProgrammableSubscriptionStats(msg) => {
                    if subscriptions
                        .get_programmable_subscription_stats(msg)
                        .await
                        .is_err()
                    {
                        tracing::warn!("Subscriptions service is not longer available. quitting");
                        break;
                    }
                }

                Msg::KillProgrammableSubscription(msg) => {
                    if subscriptions
                        .kill_programmable_subscription(msg)
                        .await
                        .is_err()
                    {
                        tracing::warn!("Subscriptions service is not longer available. quitting");
                        break;
                    }
                }

                Msg::ListProgrammableSubscriptions(msg) => {
                    if subscriptions
                        .list_programmable_subscriptions(msg.mail)
                        .await
                        .is_err()
                    {
                        tracing::warn!("Subscriptions service is not longer available. quitting");
                        break;
                    }
                }
            }
        }

        Ok::<_, eyre::Report>(())
    });
}
