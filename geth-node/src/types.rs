use bytes::Bytes;
use geth_mikoshi::Position;
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
pub enum Revision<A> {
    Start,
    End,
    Revision(A),
}

#[derive(Clone, Copy, Debug)]
pub enum Direction {
    Forward,
    Backward,
}

impl TryFrom<i32> for Direction {
    type Error = WrongDirectionError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Direction::Forward),
            1 => Ok(Direction::Backward),
            _ => Err(WrongDirectionError),
        }
    }
}

pub struct WrongDirectionError;

#[derive(Debug)]
pub struct Propose {
    pub id: Uuid,
    pub stream: String,
    pub position: Position,
    pub revision: u64,
    pub expected: ExpectedRevision,
    pub data: Bytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ExpectedRevision {
    Revision(u64),
    NoStream,
    Any,
    StreamsExists,
}
