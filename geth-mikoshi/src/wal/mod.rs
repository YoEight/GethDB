pub mod chunks;
pub mod data_events;

use crate::wal::data_events::DataEvents;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use std::sync::{Arc, RwLock};

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum LogEntryType {
    UserData,
    StreamDeleted,
    Unsupported(u8),
}

impl LogEntryType {
    pub fn from_raw(value: u8) -> Self {
        match value {
            0 => LogEntryType::UserData,
            1 => LogEntryType::StreamDeleted,
            x => LogEntryType::Unsupported(x),
        }
    }
}

impl LogEntryType {
    pub fn raw(&self) -> u8 {
        match self {
            LogEntryType::UserData => 0,
            LogEntryType::StreamDeleted => 1,
            LogEntryType::Unsupported(x) => *x,
        }
    }
}

pub trait LogRecord {
    fn get(bytes: Bytes) -> Self;
    fn put(&self, buffer: &mut BytesMut);
    fn r#type() -> LogEntryType;
    fn size(&self) -> usize;
}

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

    pub fn append<A: LogRecord>(&self, records: &[A]) -> io::Result<LogReceipt> {
        let mut inner = self.inner.write().unwrap();
        inner.append(records)
    }

    pub fn read_at(&self, position: u64) -> io::Result<LogEntry> {
        let inner = self.inner.read().unwrap();
        inner.read_at(position)
    }

    pub fn data_events(&self, from: u64) -> DataEvents<WAL> {
        let to = {
            let inner = self.inner.read().unwrap();
            inner.write_position()
        };

        DataEvents::new(self.clone(), from, to)
    }

    pub fn write_position(&self) -> u64 {
        let inner = self.inner.read().unwrap();
        inner.write_position()
    }
}

pub trait WriteAheadLog {
    fn append<A: LogRecord>(&mut self, records: &[A]) -> io::Result<LogReceipt>;
    fn read_at(&self, position: u64) -> io::Result<LogEntry>;
    fn write_position(&self) -> u64;
}

pub struct LogEntry {
    pub position: u64,
    pub r#type: LogEntryType,
    pub payload: Bytes,
}

impl LogEntry {
    pub fn size(&self) -> u32 {
        4 + self.payload_size() + 4
    }

    pub fn payload_size(&self) -> u32 {
        8 // position
            + 1 // type
            + self.payload.len() as u32
    }

    pub fn put(&self, buffer: &mut BytesMut) {
        let size = self.payload_size();

        buffer.put_u32_le(size);
        buffer.put_u64_le(self.position);
        buffer.put_u8(self.r#type.raw());
        buffer.put(self.payload.clone());
        buffer.put_u32_le(size);
    }

    /// Parsing is not asymmetrical with serialisation because parsing the size of the record
    /// is done directly when communicating with the storage abstraction directly.
    pub fn get(mut src: Bytes) -> Self {
        let position = src.get_u64_le();
        let r#type = LogEntryType::from_raw(src.get_u8());
        let payload = src;

        Self {
            position,
            r#type,
            payload,
        }
    }

    pub fn unmarshall<A: LogRecord>(&self) -> A {
        A::get(self.payload.clone())
    }
}

pub struct LogReceipt {
    pub position: u64,
    pub next_position: u64,
}
