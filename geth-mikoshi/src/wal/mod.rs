use std::io;
use std::sync::{Arc, RwLock};

use bytes::{Buf, BufMut, Bytes, BytesMut};

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
    indexes: BytesMut,
    committed: Vec<Bytes>,
    data: Bytes,
    ident: Bytes,
    revision: u64,
}

impl LogEntries {
    pub fn new(buffer: BytesMut) -> Self {
        Self {
            indexes: buffer,
            committed: Vec::with_capacity(32),
            data: Bytes::new(),
            ident: Bytes::new(),
            revision: 0,
        }
    }

    pub fn next(&mut self) -> Option<Entry<'_>> {
        if !self.data.has_remaining() {
            return None;
        }

        let len = self.data.get_u32_le() as usize;
        let record = self.data.copy_to_bytes(len);
        let current_revision = self.revision;
        let ident = self.ident.clone();

        self.revision += 1;

        Some(Entry {
            inner: self,
            ident,
            revision: current_revision,
            data: record,
        })
    }

    pub fn begin(&mut self, ident: Bytes, revision: u64, data: Bytes) {
        let key = mikoshi_hash(&ident);

        self.revision = revision;
        self.ident = ident;
        self.data = data;
        self.committed.clear();

        self.indexes.put_u8(0x01);
        self.indexes.put_u64_le(key);
    }

    pub fn complete(&mut self) -> Bytes {
        self.indexes.split().freeze()
    }

    fn index(&mut self, revision: u64, position: u64) {
        self.indexes.put_u64_le(revision);
        self.indexes.put_u64_le(position);
    }

    pub fn committed_events(&mut self) -> impl Iterator<Item = Bytes> + use<'_> {
        self.committed.drain(..)
    }
}

pub struct Entry<'a> {
    inner: &'a mut LogEntries,
    ident: Bytes,
    revision: u64,
    data: Bytes,
}

impl<'a> Entry<'a> {
    pub fn size(&self) -> usize {
        size_of::<u32>() // entry size
            + size_of::<u64>() // logical position
            + size_of::<u8>() // record type
            + size_of::<u64>() // revision
            + size_of::<u16>() // stream name length
            + self.ident.len() // stream name
            + size_of::<u32>() // payload size
            + self.data.len() // payload
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
        buffer.put_u32_le(self.data.len() as u32);
        buffer.extend_from_slice(&self.data);
        buffer.put_u32_le(actual_size as u32);

        let event = buffer.split().freeze();

        self.inner.committed.push(event.clone());

        event
    }
}

pub trait WriteAheadLog {
    fn append(&mut self, entries: &mut LogEntries) -> eyre::Result<LogReceipt>;

    fn read_at(&self, position: u64) -> eyre::Result<LogEntry>;

    fn write_position(&self) -> u64;
}

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

pub struct LogReceipt {
    pub start_position: u64,
    pub next_position: u64,
}
