use geth_common::{
    Direction, ExpectedRevision, Propose, Revision, WriteResult, WrongExpectedRevisionError,
};
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

#[derive(Debug)]
pub enum AppendStreamCompleted {
    Success(WriteResult),
    Failure(WrongExpectedRevisionError),
}
