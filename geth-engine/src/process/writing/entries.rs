use std::vec;

use bytes::{BufMut, Bytes, BytesMut};
use geth_common::Propose;
use geth_domain::index::BlockEntry;
use geth_mikoshi::{
    hashing::mikoshi_hash,
    wal::{LogEntries, LogEntry},
};

use crate::names::types::STREAM_DELETED;

pub struct ProposeEntries {
    pub indexes: Vec<BlockEntry>,
    pub committed: Vec<LogEntry>,
    events: vec::IntoIter<Propose>,
    current: Option<Propose>,
    ident: Bytes,
    key: u64,
    pub revision: u64,
}

impl Default for ProposeEntries {
    fn default() -> Self {
        Self {
            indexes: vec![],
            committed: Vec::with_capacity(32),
            events: vec![].into_iter(),
            ident: Bytes::new(),
            current: None,
            key: 0,
            revision: 0,
        }
    }
}

impl LogEntries for ProposeEntries {
    fn move_next(&mut self) -> bool {
        if let Some(event) = self.events.next() {
            self.current = Some(event);
            return true;
        }

        self.current = None;
        false
    }

    fn current_entry_size(&self) -> usize {
        size_of::<u64>() // revision
                    + size_of::<u16>() // stream name length
                    + self.ident.len() // stream name
                    + propose_estimate_size(self.current.as_ref().unwrap())
    }

    fn write_current_entry(&mut self, buffer: &mut bytes::BytesMut, position: u64) {
        let event = self.current.as_ref().unwrap();
        let final_position = if event.class == STREAM_DELETED {
            u64::MAX
        } else {
            position
        };

        self.indexes.push(BlockEntry {
            key: self.key,
            revision: self.revision,
            position: final_position,
        });

        buffer.put_u64_le(self.revision);
        buffer.put_u16_le(self.ident.len() as u16);
        buffer.extend_from_slice(&self.ident);
        propose_serialize(event, buffer);
    }

    fn commit(&mut self, entry: LogEntry) {
        self.committed.push(entry);
        self.revision += 1;
    }
}

impl ProposeEntries {
    pub fn new(ident: String, start_revision: u64, events: Vec<Propose>) -> Self {
        let key = mikoshi_hash(&ident);

        Self {
            indexes: vec![],
            committed: vec![],
            events: events.into_iter(),
            ident: Bytes::from(ident.into_bytes()),
            key,
            current: None,
            revision: start_revision,
        }
    }
}

fn propose_estimate_size(propose: &Propose) -> usize {
    size_of::<u128>() // id
        + size_of::<u32>() // content type
        + size_of::<u16>() // class length
        + propose.class.len()
        + size_of::<u32>() // payload size
        + propose.data.len()
}

fn propose_serialize(propose: &Propose, buffer: &mut BytesMut) {
    buffer.put_u128_le(propose.id.to_u128_le());
    buffer.put_u32_le((propose.content_type as i32) as u32);
    buffer.put_u16_le(propose.class.len() as u16);
    buffer.extend_from_slice(propose.class.as_bytes());
    buffer.put_u32_le(propose.data.len() as u32);
    buffer.extend_from_slice(&propose.data);
}
