mod esdb;
mod in_memory;

use crate::backend::in_memory::InMemoryBackend;
use crate::{BoxedSyncMikoshiStream, MikoshiStream};

use geth_common::{Direction, ExpectedRevision, Propose, Revision, WriteResult};
use std::io;
use std::path::Path;

pub trait Backend {
    fn append(
        &mut self,
        stream_name: String,
        expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> eyre::Result<WriteResult>;

    fn read(
        &mut self,
        stream_name: String,
        starting: Revision<u64>,
        direction: Direction,
    ) -> eyre::Result<BoxedSyncMikoshiStream>;
}

pub fn in_memory_backend() -> InMemoryBackend {
    InMemoryBackend::default()
}

pub fn esdb_backend(root: impl AsRef<Path>) -> eyre::Result<esdb::BlockingEsdbBackend> {
    esdb::BlockingEsdbBackend::new(root)
}

#[async_trait::async_trait]
pub trait AsyncBackend {
    async fn append(
        &mut self,
        stream_name: String,
        expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> io::Result<WriteResult>;

    async fn read(
        &mut self,
        stream_name: String,
        starting: Revision<u64>,
        direction: Direction,
    ) -> io::Result<MikoshiStream>;
}
