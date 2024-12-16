use std::io;
use std::sync::{Arc, RwLock};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::wal::entries::EntryIter;

pub mod chunks;
pub mod entries;

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

    pub fn append(&self, entries: &mut LogEntries) -> io::Result<LogReceipt>
where {
        let mut inner = self.inner.write().unwrap();
        inner.append(entries)
    }

    pub fn read_at(&self, position: u64) -> io::Result<LogEntry> {
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
    data: Bytes,
    ident: Bytes,
    index: bool,
    indexes: Vec<(u64, u64)>,
    revision: u64,
}

impl LogEntries {
    pub fn new(ident: Bytes, revision: u64, index: bool, data: Bytes) -> Self {
        Self {
            data,
            ident,
            index,
            indexes: vec![],
            revision,
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

    pub fn complete(self) -> Vec<(u64, u64)> {
        self.indexes
    }

    fn index(&mut self, revision: u64, position: u64) {
        if self.index {
            self.indexes.push((revision, position));
        }
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
        size_of::<u64>()
            + size_of::<u64>()
            + size_of::<u16>()
            + self.ident.len()
            + size_of::<u32>()
            + self.data.len()
    }

    pub fn commit(mut self, buffer: &mut BytesMut, position: u64) -> Bytes {
        self.inner.index(self.revision, position);

        buffer.put_u64_le(position);
        buffer.put_u64_le(self.revision);
        buffer.put_u16_le(self.ident.len() as u16);
        buffer.extend_from_slice(&self.ident);
        buffer.put_u32_le(self.data.len() as u32);
        buffer.extend_from_slice(&self.data);

        buffer.split().freeze()
    }
}

pub trait WriteAheadLog {
    fn append(&mut self, entries: &mut LogEntries) -> io::Result<LogReceipt>;

    fn read_at(&self, position: u64) -> io::Result<LogEntry>;

    fn write_position(&self) -> u64;
}

pub struct LogEntry {
    pub position: u64,
    pub payload: Bytes,
}

impl LogEntry {
    pub fn size(&self) -> u32 {
        4 + self.payload_size() + 4
    }

    pub fn payload_size(&self) -> u32 {
        8 // position
            + self.payload.len() as u32
    }

    pub fn put(&self, buffer: &mut BytesMut) {
        let size = self.payload_size();

        buffer.put_u32_le(size);
        buffer.put_u64_le(self.position);
        buffer.put(self.payload.clone());
        buffer.put_u32_le(size);
    }

    /// Parsing is not symmetrical with serialisation because parsing the size of the record
    /// is done directly when communicating with the storage abstraction directly.
    pub fn get(mut src: Bytes) -> Self {
        let position = src.get_u64_le();
        let payload = src;

        Self { position, payload }
    }
}

pub struct LogReceipt {
    pub start_position: u64,
    pub next_position: u64,
}
