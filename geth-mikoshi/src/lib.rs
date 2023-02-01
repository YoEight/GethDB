use bytes::Bytes;
use eyre::bail;
use geth_common::Position;
use uuid::Uuid;

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
