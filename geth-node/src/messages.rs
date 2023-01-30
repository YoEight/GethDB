use crate::types::{Direction, ExpectedRevision, Propose, Revision, WriteResult};
use geth_mikoshi::MikoshiStream;
use uuid::Uuid;

#[derive(Debug)]
pub struct ReadStream {
    pub correlation: Uuid,
    pub stream_name: String,
    pub direction: Direction,
    pub starting: Revision<u64>,
    pub count: usize,
}

pub struct ReadStreamCompleted {
    pub correlation: Uuid,
    pub reader: MikoshiStream,
}

#[derive(Debug)]
pub struct AppendStream {
    pub correlation: Uuid,
    pub stream_name: String,
    pub events: Vec<Propose>,
    pub expected: ExpectedRevision,
}

pub struct AppendStreamCompleted {
    pub correlation: Uuid,
    pub result: WriteResult,
    pub next_revision: Option<u64>,
}
