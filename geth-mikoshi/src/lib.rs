use std::collections::HashMap;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use eyre::bail;
use geth_common::{Position, Record};
use uuid::Uuid;

#[derive(Debug)]
struct Entry {
    pub id: Uuid,
    pub stream: String,
    pub revision: u64,
    pub data: Bytes,
    pub position: Position,
    pub created: DateTime<Utc>,
}

#[derive(Default)]
pub struct Mikoshi {
    log: Vec<Entry>,
    indexes: HashMap<String, usize>,
    revisions: HashMap<String, u64>,
    log_position: u64,
}

impl Mikoshi {
    pub fn in_memory() -> Self {
        Default::default()
    }
}

pub struct MikoshiStream {}

impl MikoshiStream {
    pub async fn next(&mut self) -> eyre::Result<Option<Record>> {
        bail!("Not implemented")
    }
}
