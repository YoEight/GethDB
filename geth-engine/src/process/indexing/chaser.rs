use crate::process::{ProcessEnv, ProcessRawEnv, Runnable, RunnableRaw};
use geth_domain::Lsm;
use geth_mikoshi::storage::Storage;
use std::io;
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

impl<S> RunnableRaw for Chaser<S>
where
    S: Storage + Sync + Send + 'static,
{
    fn name(&self) -> &'static str {
        "chaser"
    }

    fn run(self: Box<Self>, env: ProcessRawEnv) -> io::Result<()> {
        Ok(())
    }
}
