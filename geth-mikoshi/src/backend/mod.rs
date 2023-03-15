mod in_memory;

use crate::backend::in_memory::InMemoryBackend;
use crate::MikoshiStream;
use geth_common::{Direction, ExpectedRevision, Propose, Revision, WriteResult};

pub trait Backend {
    fn append(
        &mut self,
        stream_name: String,
        expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> WriteResult;

    fn read(
        &self,
        stream_name: String,
        starting: Revision<u64>,
        direction: Direction,
    ) -> MikoshiStream;
}

pub fn in_memory_backend() -> InMemoryBackend {
    InMemoryBackend::default()
}
