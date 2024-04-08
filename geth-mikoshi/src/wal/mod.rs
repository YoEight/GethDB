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

    pub fn append(&self, entry: &[u8]) -> io::Result<LogReceipt> {
        let mut inner = self.inner.write().unwrap();
        inner.append(entry)
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

pub trait WriteAheadLog {
    fn append(&mut self, entry: &[u8]) -> io::Result<LogReceipt>;
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

    /// Parsing is not asymmetrical with serialisation because parsing the size of the record
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
