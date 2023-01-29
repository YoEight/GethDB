use bytes::Bytes;
use eyre::bail;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Position(u64);

impl Position {
    pub fn raw(&self) -> u64 {
        self.0
    }
}

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
    pub async fn next(&mut self) -> eyre::Result<Option<Record>> {
        bail!("Not implemented")
    }
}
