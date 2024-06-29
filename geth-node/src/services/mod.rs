use tokio::task::JoinHandle;

use geth_common::Client;
use geth_domain::Lsm;
use geth_mikoshi::storage::Storage;

mod index;

pub struct Service {
    name: &'static str,
    handle: JoinHandle<eyre::Result<()>>,
}

pub struct Services {
    inner: Vec<Service>,
}

pub fn start<C, S>(client: C, lsm: Lsm<S>) -> Services
where
    C: Client + Send + 'static,
    S: Storage + Sync + Send + 'static,
{
    let mut inner = vec![];

    inner.push(Service {
        name: "indexing",
        handle: tokio::spawn(index::indexing(client, lsm)),
    });

    Services { inner }
}
