use std::time::Duration;

use tokio::task::JoinHandle;

use geth_common::Client;
use geth_mikoshi::storage::Storage;

use crate::domain::index::IndexRef;
use crate::process::SubscriptionsClient;

mod index;

pub struct Service {
    name: &'static str,
    handle: JoinHandle<eyre::Result<()>>,
}

pub struct Services {
    inner: Vec<Service>,
}

impl Services {
    pub async fn exited(self) {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            if self.inner.iter().any(|svc| svc.handle.is_finished()) {
                break;
            }
        }

        for svc in self.inner {
            if svc.handle.is_finished() {
                if let Err(e) = svc.handle.await.unwrap() {
                    tracing::error!("service '{}' exited with an error: {}", svc.name, e);
                } else {
                    tracing::info!("service '{}' completed", svc.name);
                }
            }
        }
    }
}

pub fn start<C, S>(client: C, index: IndexRef<S>, sub_client: SubscriptionsClient) -> Services
where
    C: Client + Send + 'static,
    S: Storage + Sync + Send + 'static,
{
    let inner = vec![Service {
        name: "indexing",
        handle: tokio::spawn(index::indexing(client, index, sub_client)),
    }];

    Services { inner }
}
