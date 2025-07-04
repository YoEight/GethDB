use crate::storage::{FileId, Storage};
use crate::wal::chunks::ChunkContainer;
use crate::wal::LogReceipt;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;

use super::{LogEntries, LogEntry};

const ENTRY_PREFIX_SIZE: usize = size_of::<u32>() // pre-entry size
    + ENTRY_HEADER_SIZE;

const ENTRY_HEADER_SIZE: usize = size_of::<u64>() // log position
    + size_of::<u8>(); // log type

const ENTRY_META_SIZE: usize = ENTRY_PREFIX_SIZE + size_of::<u32>(); // post-entry size;

pub struct LogWriter {
    container: ChunkContainer,
    buffer: BytesMut,
    writer: u64,
}

impl LogWriter {
    pub fn load(container: ChunkContainer, buffer: BytesMut) -> eyre::Result<LogWriter> {
        let storage = container.storage();
        let mut writer = 0u64;

        if !storage.exists(FileId::writer_chk())? {
            flush_writer_chk(storage, writer)?;
        } else {
            writer = storage
                .read_from(FileId::writer_chk(), 0, size_of::<u64>())?
                .get_u64_le();
        }

        Ok(Self {
            container,
            buffer,
            writer,
        })
    }

    pub fn append<E>(&mut self, entries: &mut E) -> eyre::Result<LogReceipt>
    where
        E: LogEntries,
    {
        let mut position = self.writer;
        let starting_position = position;
        let storage = self.container.storage();
        let mut chunk = self.container.ongoing()?;
        let expected_count = entries.expected_count();
        let mut count = 0usize;

        while entries.move_next() {
            let entry_size = entries.current_entry_size();
            let actual_size = entry_size + ENTRY_META_SIZE;
            let projected_next_logical_position = actual_size as u64 + position;

            // Chunk is full, and we need to flush previous data we accumulated. We also create a new
            // chunk for next writes.
            if !chunk.contains_log_position(projected_next_logical_position) {
                let remaining_space = chunk.remaining_space_from(position);
                chunk = self.container.new_chunk(&mut self.buffer, position)?;
                position += remaining_space;
            }

            let reported_size = (entry_size + ENTRY_HEADER_SIZE) as u32;
            self.buffer.reserve(actual_size);
            self.buffer.put_u32_le(reported_size);
            self.buffer.put_u64_le(position);
            self.buffer.put_u8(0);
            let mut payload_buffer = self.buffer.split_off(ENTRY_PREFIX_SIZE);

            entries.write_current_entry(&mut payload_buffer, position);

            if payload_buffer.len() != entry_size {
                eyre::bail!(
                    "payload size mismatch: expected {}, got {}",
                    entry_size,
                    payload_buffer.len()
                );
            }

            payload_buffer.put_u32_le(reported_size);
            self.buffer.unsplit(payload_buffer);
            let record = self.buffer.split().freeze();
            let payload = record.slice(ENTRY_PREFIX_SIZE..record.len() - size_of::<u32>());
            let local_offset = chunk.raw_position(position);
            let entry = LogEntry {
                position,
                r#type: 0,
                payload,
            };

            count += 1;
            position += actual_size as u64;
            storage.write_to(chunk.file_id(), local_offset, record)?;
            entries.commit(entry);
        }

        if count != expected_count {
            eyre::bail!(
                "expected {} entries, but only wrote {}",
                expected_count,
                count
            );
        }

        flush_writer_chk(storage, self.writer)?;
        self.writer = position;

        Ok(LogReceipt {
            start_position: starting_position,
            next_position: self.writer,
        })
    }

    pub fn writer_position(&self) -> u64 {
        self.writer
    }
}

fn flush_writer_chk(storage: &Storage, log_pos: u64) -> io::Result<()> {
    storage.write_to(
        FileId::writer_chk(),
        0,
        Bytes::copy_from_slice(log_pos.to_le_bytes().as_slice()),
    )
}
