use std::io;

use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use prost::Message;
use uuid::Uuid;

pub use index::{Lsm, LsmSettings};

pub use crate::append_propose::AppendProposes;
use crate::binary::events::Events;

mod append_propose;
pub mod binary;
pub mod index;
mod iter;

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum ExpectedRevision {
    Any,
    Empty,
    Exists,
    Revision(u64),
}

pub struct ProposedEvent<'a> {
    pub r#type: &'a str,
    pub payload: &'a [u8],
}

pub struct RecordedEvent {
    pub id: Uuid,
    pub revision: u64,
    pub stream_name: String,
    pub class: String,
    pub created: DateTime<Utc>,
    pub data: Bytes,
    pub metadata: Bytes,
}

impl RecordedEvent {
    pub fn from(inner: binary::events::RecordedEvent) -> RecordedEvent {
        Self {
            id: inner.id.into(),
            revision: inner.revision,
            stream_name: inner.stream_name,
            class: inner.class,
            created: Utc.timestamp_opt(inner.created, 0).unwrap(),
            data: inner.data,
            metadata: inner.metadata,
        }
    }
}

pub fn parse_event(payload: &[u8]) -> eyre::Result<Events> {
    let evt = Events::decode(payload)?;
    Ok(evt)
}

pub fn parse_event_io(payload: &[u8]) -> io::Result<Events> {
    parse_event(payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
