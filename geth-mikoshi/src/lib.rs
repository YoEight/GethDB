use tokio::sync::mpsc;

use geth_common::Record;

pub use crate::storage::fs::FileSystemStorage;
pub use crate::storage::in_mem::InMemoryStorage;

mod constants;
pub mod hashing;
pub mod storage;
pub mod wal;

pub struct MikoshiStream {
    inner: mpsc::UnboundedReceiver<Record>,
}

impl MikoshiStream {
    pub fn empty() -> Self {
        let (_, inner) = mpsc::unbounded_channel();

        Self { inner }
    }

    pub fn new(inner: mpsc::UnboundedReceiver<Record>) -> Self {
        Self { inner }
    }

    pub fn from_vec(entries: Vec<Record>) -> Self {
        let (sender, inner) = mpsc::unbounded_channel();

        for entry in entries {
            let _ = sender.send(entry);
        }

        Self { inner }
    }

    pub async fn next(&mut self) -> eyre::Result<Option<Record>> {
        if let Some(record) = self.inner.recv().await {
            return Ok(Some(record));
        }

        Ok(None)
    }
}
