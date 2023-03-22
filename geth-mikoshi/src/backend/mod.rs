mod esdb;
mod in_memory;

use crate::backend::in_memory::InMemoryBackend;
use crate::MikoshiStream;
use async_trait::async_trait;
use geth_common::{Direction, ExpectedRevision, Propose, Revision, WriteResult};
use std::io;

pub trait Backend {
    fn append(
        &mut self,
        stream_name: String,
        expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> io::Result<WriteResult>;

    fn read(
        &mut self,
        stream_name: String,
        starting: Revision<u64>,
        direction: Direction,
    ) -> io::Result<MikoshiStream>;
}

pub fn in_memory_backend() -> InMemoryBackend {
    InMemoryBackend::default()
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
