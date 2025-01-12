use bytes::{Buf, Bytes, BytesMut};

pub mod chunks;
mod log_reader;
mod log_writer;

pub use log_reader::LogReader;
pub use log_writer::LogWriter;

pub const LOG_ENTRY_HEADER_SIZE: usize = size_of::<u64>() + size_of::<u8>(); // position and type

pub trait LogEntries {
    fn move_next(&mut self) -> bool;
    fn current_entry_size(&self) -> usize;
    fn write_current_entry(&mut self, buffer: &mut BytesMut, position: u64);
    fn expected_count(&self) -> usize;
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

impl TryFrom<Bytes> for LogEntry {
    type Error = eyre::Report;

    fn try_from(mut src: Bytes) -> eyre::Result<Self> {
        if src.remaining() < LOG_ENTRY_HEADER_SIZE {
            eyre::bail!("bytes buffer is too short to contain a valid log entry");
        }

        let position = src.get_u64_le();
        let r#type = src.get_u8();
        let payload = src;

        Ok(Self {
            position,
            r#type,
            payload,
        })
    }
}

pub struct LogReceipt {
    pub start_position: u64,
    pub next_position: u64,
}
