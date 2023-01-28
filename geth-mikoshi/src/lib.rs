use bytes::Bytes;
use eyre::bail;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Position(u64);

#[derive(Debug)]
pub struct Record {
    pub id: Uuid,
    pub stream: String,
    pub position: Position,
    pub revision: u64,
    pub data: Bytes,
}

pub struct MikoshiStream {}

impl MikoshiStream {
    async fn next(&mut self) -> eyre::Result<Record> {
        bail!("Not implemented")
    }
}
