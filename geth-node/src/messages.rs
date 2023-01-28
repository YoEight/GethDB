use crate::types::Direction;
use geth_mikoshi::MikoshiStream;
use uuid::Uuid;

pub struct ReadStream {
    correlation: Uuid,
    stream: String,
    direction: Direction,
    count: usize,
}

pub struct ReadStreamCompleted {
    correlation: Uuid,
    reader: MikoshiStream,
}
