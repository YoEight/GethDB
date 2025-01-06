use bytes::{Buf, Bytes, BytesMut};
use geth_common::{Position, Record};
use uuid::Uuid;

pub mod chunks;
mod log_reader;
mod log_writer;

pub use log_reader::LogReader;
pub use log_writer::LogWriter;

pub trait LogEntries {
    fn move_next(&mut self) -> bool;
    fn current_entry_size(&self) -> usize;
    fn write_current_entry(&mut self, buffer: &mut BytesMut, position: u64);
    fn commit(&mut self, _: LogEntry) {}
}

#[derive(Clone, Debug)]
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

impl From<LogEntry> for Record {
    fn from(mut entry: LogEntry) -> Record {
        let revision = entry.payload.get_u64_le();
        let stream_name_len = entry.payload.get_u16_le() as usize;
        let stream_name = unsafe {
            String::from_utf8_unchecked(entry.payload.copy_to_bytes(stream_name_len).to_vec())
        };

        let id = Uuid::from_u128_le(entry.payload.get_u128_le());
        let type_len = entry.payload.get_u16_le() as usize;
        let r#type =
            unsafe { String::from_utf8_unchecked(entry.payload.copy_to_bytes(type_len).to_vec()) };

        entry.payload.advance(size_of::<u32>()); // skip the payload size

        Record {
            id,
            r#type,
            stream_name,
            position: Position(entry.position),
            revision,
            data: entry.payload,
        }
    }
}

pub struct LogReceipt {
    pub start_position: u64,
    pub next_position: u64,
}
