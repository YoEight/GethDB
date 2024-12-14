use geth_common::{ExpectedRevision, IteratorIO};
use geth_domain::index::BlockEntry;
use geth_domain::Lsm;
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::storage::Storage;
use std::io;
use std::sync::{Arc, RwLock};

pub type RevisionCache = moka::sync::Cache<String, u64>;

pub fn new_revision_cache() -> RevisionCache {
    moka::sync::Cache::<String, u64>::builder()
        .max_capacity(10_000)
        .name("revision-cache")
        .build()
}

#[derive(Clone)]
pub struct IndexRef<S> {
    pub inner: Arc<RwLock<Index<S>>>,
    storage: S,
}

impl<S> IndexRef<S> {
    pub fn storage(&self) -> &S {
        &self.storage
    }
}

#[derive(Clone)]
pub struct Index<S> {
    pub lsm: Lsm<S>,
    pub revision_cache: RevisionCache,
}

impl<S> Index<S>
where
    S: Storage + Send + Sync + 'static,
{
    pub fn new(lsm: Lsm<S>) -> Self {
        Self {
            lsm,
            revision_cache: new_revision_cache(),
        }
    }

    pub fn guarded(self) -> IndexRef<S> {
        let storage = self.lsm.storage.clone();

        IndexRef {
            inner: Arc::new(RwLock::new(self)),
            storage,
        }
    }

    pub fn stream_current_revision(&self, stream_name: &str) -> io::Result<CurrentRevision> {
        let stream_key = mikoshi_hash(stream_name);
        let current_revision = if let Some(current) = self.revision_cache.get(stream_name) {
            CurrentRevision::Revision(current)
        } else {
            let revision = self
                .lsm
                .highest_revision(stream_key)?
                .map_or_else(|| CurrentRevision::NoStream, CurrentRevision::Revision);

            if let CurrentRevision::Revision(rev) = revision {
                self.revision_cache.insert(stream_name.to_string(), rev);
            }

            revision
        };

        Ok(current_revision)
    }

    pub fn cache_stream_revision(&self, stream_name: String, revision: u64) {
        self.revision_cache.insert(stream_name, revision);
    }

    pub fn scan_forward(
        &self,
        stream_name: &str,
        start: u64,
        count: usize,
    ) -> impl IteratorIO<Item = BlockEntry> + use<'_, S> {
        self.lsm
            .scan_forward(mikoshi_hash(stream_name), start, count)
    }

    pub fn scan_backward(
        &self,
        stream_name: &str,
        start: u64,
        count: usize,
    ) -> impl IteratorIO<Item = BlockEntry> + use<'_, S> {
        self.lsm
            .scan_backward(mikoshi_hash(stream_name), start, count)
    }

    pub fn register(&mut self, stream_hash: u64, revision: u64, position: u64) -> eyre::Result<()> {
        self.lsm.put_single(stream_hash, revision, position)?;
        Ok(())
    }

    pub fn register_multiple(
        &mut self,
        values: impl IntoIterator<Item = (u64, u64, u64)>,
    ) -> eyre::Result<()> {
        self.lsm.put_values(values)?;
        Ok(())
    }
}

#[derive(Copy, Clone)]
pub enum CurrentRevision {
    NoStream,
    Revision(u64),
}

impl CurrentRevision {
    pub fn next_revision(self) -> u64 {
        match self {
            CurrentRevision::NoStream => 0,
            CurrentRevision::Revision(r) => r + 1,
        }
    }

    pub fn as_expected(self) -> ExpectedRevision {
        match self {
            CurrentRevision::NoStream => ExpectedRevision::NoStream,
            CurrentRevision::Revision(v) => ExpectedRevision::Revision(v),
        }
    }

    pub fn is_deleted(&self) -> bool {
        if let CurrentRevision::Revision(r) = self {
            return *r == u64::MAX;
        }

        false
    }

    pub fn revision(self) -> Option<u64> {
        if let CurrentRevision::Revision(r) = self {
            return Some(r);
        }

        None
    }
}
