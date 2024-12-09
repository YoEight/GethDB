use crate::process::{ProcessEnv, Runnable};
use geth_domain::Lsm;
use geth_mikoshi::storage::Storage;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};

pub struct Chaser<S> {
    chk: Arc<AtomicU64>,
    lsm: Arc<RwLock<Lsm<S>>>,
}

impl<S> Chaser<S> {
    pub fn new(chk: Arc<AtomicU64>, lsm: Arc<RwLock<Lsm<S>>>) -> Self {
        Self { chk, lsm }
    }
}

#[async_trait::async_trait]
impl<S> Runnable for Chaser<S>
where
    S: Storage + Sync + Send + 'static,
{
    fn name(&self) -> &'static str {
        "chaser"
    }

    async fn run(self: Box<Self>, env: ProcessEnv) {
        todo!()
    }
}
