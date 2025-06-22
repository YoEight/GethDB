use std::future::Future;

use tokio::{runtime::Handle, sync::mpsc::UnboundedReceiver, task::JoinHandle};

use crate::{
    process::{subscription::SubscriptionClient, Item},
    IndexClient, ManagerClient, Options, Proc,
};

pub struct Managed {
    pub queue: UnboundedReceiver<Item>,
}

pub struct Raw {
    pub queue: std::sync::mpsc::Receiver<Item>,
    pub handle: Handle,
}

pub struct ProcessEnv<A> {
    pub proc: Proc,
    pub client: ManagerClient,
    pub options: Options,
    inner: A,
}

impl<A> ProcessEnv<A> {
    pub fn new(proc: Proc, client: ManagerClient, options: Options, inner: A) -> Self {
        Self {
            proc,
            client,
            options,
            inner,
        }
    }
}

impl ProcessEnv<Managed> {
    pub async fn recv(&mut self) -> Option<Item> {
        if let Some(item) = self.inner.queue.recv().await {
            if item.is_shutdown() {
                return None;
            }

            return Some(item);
        }

        None
    }
}

impl ProcessEnv<Raw> {
    pub fn recv(&self) -> Option<Item> {
        if let Ok(item) = self.inner.queue.recv() {
            if item.is_shutdown() {
                return None;
            }

            return Some(item);
        }

        None
    }

    pub fn spawn_blocking<F, R>(&self, func: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.inner.handle.spawn_blocking(func)
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.inner.handle.block_on(future)
    }

    pub fn new_index_client(&self) -> eyre::Result<IndexClient> {
        self.block_on(self.client.new_index_client())
    }

    pub fn new_subscription_client(&self) -> eyre::Result<SubscriptionClient> {
        self.block_on(self.client.new_subscription_client())
    }
}
