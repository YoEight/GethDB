use bytes::Bytes;
use eyre::bail;
use geth_common::{Position, Record};
use uuid::Uuid;

pub struct MikoshiStream {}

impl MikoshiStream {
    pub async fn next(&mut self) -> eyre::Result<Option<Record>> {
        bail!("Not implemented")
    }
}
