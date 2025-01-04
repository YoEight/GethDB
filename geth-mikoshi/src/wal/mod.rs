use std::sync::{Arc, RwLock};
use std::{mem, vec};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use geth_common::{Position, Propose, Record};
use uuid::Uuid;

use crate::wal::entries::EntryIter;

pub mod chunks;
pub mod entries;
mod log_reader;
mod log_writer;

use crate::hashing::mikoshi_hash;
pub use log_reader::LogReader;
pub use log_writer::LogWriter;

pub struct WALRef<A> {
    inner: Arc<RwLock<A>>,
}

impl<A> Clone for WALRef<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<WAL: WriteAheadLog> WALRef<WAL> {
    pub fn new(inner: WAL) -> Self {
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub fn append(&self, entries: &mut LogEntries) -> eyre::Result<LogReceipt>
where {
        let mut inner = self.inner.write().unwrap();
        inner.append(entries)
    }

    pub fn read_at(&self, position: u64) -> eyre::Result<LogEntry> {
        let inner = self.inner.read().unwrap();
        inner.read_at(position)
    }

    pub fn entries(&self, from: u64) -> EntryIter<WAL> {
        let to = {
            let inner = self.inner.read().unwrap();
            inner.write_position()
        };

        EntryIter::new(self.clone(), from, to)
    }

    pub fn write_position(&self) -> u64 {
        let inner = self.inner.read().unwrap();
        inner.write_position()
    }
}

pub struct LogEntries {
    indexes: Vec<(u64, u64, u64)>,
    committed: Vec<LogEntry>,
    events: vec::IntoIter<Propose>,
    ident: Bytes,
    key: u64,
    revision: u64,
}

impl LogEntries {
    pub fn new() -> Self {
        Self {
            indexes: vec![],
            committed: Vec::with_capacity(32),
            events: vec![].into_iter(),
            ident: Bytes::new(),
            key: 0,
            revision: 0,
        }
    }

    pub fn next(&mut self) -> Option<Entry<'_>> {
        let propose = self.events.next()?;
        let ident = self.ident.clone();
        let current_revision = self.revision;

        self.revision += 1;

        Some(Entry {
            inner: self,
            ident,
            revision: current_revision,
            event: propose,
        })
    }

    pub fn begin(&mut self, ident: String, revision: u64, events: Vec<Propose>) {
        self.key = mikoshi_hash(&ident);
        self.revision = revision;
        self.ident = ident.into_bytes().into();
        self.events = events.into_iter();
        self.committed.clear();
    }

    pub fn complete(&mut self) -> impl Iterator<Item = (u64, u64, u64)> + use<'_> {
        self.indexes.drain(..)
    }

    fn index(&mut self, revision: u64, position: u64) {
        self.indexes.push((self.key, revision, position));
    }

    pub fn committed_events(&mut self) -> Vec<LogEntry> {
        mem::take(&mut self.committed)
    }
}

pub struct Entry<'a> {
    inner: &'a mut LogEntries,
    ident: Bytes,
    revision: u64,
    event: Propose,
}

impl<'a> Entry<'a> {
    pub fn size(&self) -> usize {
        size_of::<u32>() // entry size
            + size_of::<u64>() // logical position
            + size_of::<u8>() // record type
            + size_of::<u64>() // revision
            + size_of::<u16>() // stream name length
            + self.ident.len() // stream name
            + propose_estimate_size(&self.event)
            + size_of::<u32>() // entry size
    }

    pub fn commit(self, buffer: &mut BytesMut, position: u64) -> Bytes {
        self.inner.index(self.revision, position);
        let size = self.size();

        let actual_size = size - 2 * size_of::<u32>(); // we don't count the 32bits encoded entry size at the front and back of the log entry.
        buffer.put_u32_le(actual_size as u32); // we don't count the 32bits encoded entry size at the front and back of the log entry.
        buffer.put_u64_le(position);
        buffer.put_u8(0);
        buffer.put_u64_le(self.revision);
        buffer.put_u16_le(self.ident.len() as u16);
        buffer.extend_from_slice(&self.ident);
        propose_serialize(&self.event, buffer);
        buffer.put_u32_le(actual_size as u32);

        let event = buffer.split().freeze();
        let payload = event.slice(
            size_of::<u32>() + size_of::<u64>() + size_of::<u8>()..event.len() - size_of::<u32>(),
        );

        self.inner.committed.push(LogEntry {
            position,
            r#type: 0,
            payload,
        });

        event
    }
}

fn propose_estimate_size(propose: &Propose) -> usize {
    size_of::<u128>() // id
        + size_of::<u16>() // type length
        + propose.r#type.len() // type string
        + size_of::<u32>() // payload size
        + propose.data.len()
}

fn propose_serialize(propose: &Propose, buffer: &mut BytesMut) {
    buffer.put_u128_le(propose.id.to_u128_le());
    buffer.put_u16_le(propose.r#type.len() as u16);
    buffer.extend_from_slice(propose.r#type.as_bytes());
    buffer.put_u32_le(propose.data.len() as u32);
    buffer.extend_from_slice(&propose.data);
}

pub trait WriteAheadLog {
    fn append(&mut self, entries: &mut LogEntries) -> eyre::Result<LogReceipt>;

    fn read_at(&self, position: u64) -> eyre::Result<LogEntry>;

    fn write_position(&self) -> u64;
}

#[derive(Clone)]
pub struct LogEntry {
    pub position: u64,
    pub r#type: u8,
    pub payload: Bytes,
}

impl LogEntry {
    pub fn size(&self) -> usize {
        size_of::<u32>() // entry size
            + size_of::<u8>() // entry type
            + self.payload_size()
            + size_of::<u32>() // entry size
    }

    pub fn payload_size(&self) -> usize {
        size_of::<u64>() // position
            + self.payload.len()
    }

    /// Parsing is not symmetrical with serialisation because parsing the size of the record
    /// is done directly when communicating with the storage abstraction directly.
    pub fn get(mut src: Bytes) -> Self {
        let position = src.get_u64_le();
        let r#type = src.get_u8();
        let payload = src;

        Self {
            position,
            r#type,
            payload,
        }
    }
}

impl Into<Record> for LogEntry {
    fn into(mut self) -> Record {
        let revision = self.payload.get_u64_le();
        let stream_name_len = self.payload.get_u16_le() as usize;
        let stream_name = unsafe {
            String::from_utf8_unchecked(self.payload.copy_to_bytes(stream_name_len).to_vec())
        };

        let id = Uuid::from_u128_le(self.payload.get_u128_le());
        let type_len = self.payload.get_u16_le() as usize;
        let r#type =
            unsafe { String::from_utf8_unchecked(self.payload.copy_to_bytes(type_len).to_vec()) };

        self.payload.advance(size_of::<u32>()); // skip the payload size

        Record {
            id,
            r#type,
            stream_name,
            position: Position(self.position),
            revision,
            data: self.payload,
        }
    }
}

pub struct LogReceipt {
    pub start_position: u64,
    pub next_position: u64,
}
