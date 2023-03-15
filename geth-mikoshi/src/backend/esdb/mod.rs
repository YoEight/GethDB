use crate::backend::Backend;
use crate::MikoshiStream;
use geth_common::{Direction, ExpectedRevision, Propose, Revision, WriteResult};

mod manager;
pub mod parsing;
pub mod types;
mod utils;

pub struct EsdbBackend {}

impl Backend for EsdbBackend {
    fn append(
        &mut self,
        stream_name: String,
        expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> WriteResult {
        todo!()
    }

    fn read(
        &self,
        stream_name: String,
        starting: Revision<u64>,
        direction: Direction,
    ) -> MikoshiStream {
        todo!()
    }
}
