use crate::types::{Direction, Revision};
use geth_mikoshi::MikoshiStream;
use uuid::Uuid;

pub struct ReadStream {
    pub correlation: Uuid,
    pub stream_name: String,
    pub direction: Direction,
    pub starting: Revision<u64>,
    pub count: usize,
}

pub struct ReadStreamCompleted {
    correlation: Uuid,
    reader: MikoshiStream,
}
