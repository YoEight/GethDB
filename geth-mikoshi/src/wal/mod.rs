pub mod chunks;
#[cfg(test)]
mod tests;

use bytes::Bytes;
pub use chunks::manager::ChunkManager;
use std::io;

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum LogEntryType {
    UserData,
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

pub struct LogReceipt {
    pub position: u64,
    pub next_position: u64,
}
