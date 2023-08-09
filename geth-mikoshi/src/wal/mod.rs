pub mod chunks;
#[cfg(test)]
mod tests;

use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use chunks::manager::ChunkManager;
use std::io;

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum LogEntryType {
    UserData,
    Unsupported(u8),
}

impl LogEntryType {
    pub fn from_raw(value: u8) -> Self {
        match value {
            0 => LogEntryType::UserData,
            x => LogEntryType::Unsupported(x),
        }
    }
}

impl LogEntryType {
    pub fn raw(&self) -> u8 {
        match self {
            LogEntryType::UserData => 0,
            LogEntryType::Unsupported(x) => *x,
        }
    }
}

pub trait WriteAheadLog {
    fn append(&mut self, r#type: LogEntryType, payload: Bytes) -> io::Result<LogReceipt>;
    fn read_at(&mut self, position: u64) -> io::Result<LogEntry>;
}

pub struct LogEntry {
    pub position: u64,
    pub r#type: LogEntryType,
    pub payload: Bytes,
}

impl LogEntry {
    pub fn size(&self) -> u64 {
        let entry_size = 8 // position
                + 1 // type
                + self.payload.len();

        8 + entry_size as u64 + 8
    }

    pub fn put(&self, buffer: &mut BytesMut) {
        let size = self.size();

        buffer.put_u64_le(size);
        buffer.put_u64_le(self.position);
        buffer.put_u8(self.r#type.raw());
        buffer.put(self.payload.clone());
        buffer.put_u64_le(size);
    }

    pub fn get(mut src: Bytes) -> Self {
        let size = src.get_u64_le();
        let position = src.get_u64_le();
        let r#type = LogEntryType::from_raw(src.get_u8());
        let payload = src.copy_to_bytes((size - 8 - 8 - 1 - 8) as usize);

        assert_eq!(
            size,
            src.get_u64_le(),
            "We are testing the log entry has a valid frame"
        );

        Self {
            position,
            r#type,
            payload,
        }
    }
}

pub struct LogReceipt {
    pub position: u64,
    pub next_position: u64,
}
