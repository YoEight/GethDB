use crate::storage::{FileId, Storage};
use crate::wal::chunks::ChunkContainer;
use crate::wal::{LogEntries, LogReceipt};
use bytes::{Buf, Bytes, BytesMut};
use std::io;

pub struct LogWriter<S> {
    container: ChunkContainer<S>,
    buffer: BytesMut,
    writer: u64,
}

impl<S> LogWriter<S>
where
    S: Storage,
{
    pub fn load(container: ChunkContainer<S>, buffer: BytesMut) -> eyre::Result<LogWriter<S>> {
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

    pub fn append(&mut self, entries: &mut LogEntries) -> eyre::Result<LogReceipt> {
        let mut position = self.writer;
        let starting_position = position;
        let storage = self.container.storage();

        let mut chunk = self.container.ongoing()?;
        while let Some(entry) = entries.next_entry() {
            let entry_size = entry.size();
            let projected_next_logical_position = entry_size as u64 + position;

            // Chunk is full, and we need to flush previous data we accumulated. We also create a new
            // chunk for next writes.
            if !chunk.contains_log_position(projected_next_logical_position) {
                let remaining_space = chunk.remaining_space_from(position);
                chunk = self.container.new_chunk(&mut self.buffer, position)?;
                position += remaining_space;
            }

            let record = entry.commit(&mut self.buffer, position);
            let local_offset = chunk.raw_position(position);
            position += entry_size as u64;
            storage.write_to(chunk.file_id(), local_offset, record)?;

            self.writer = position;
        }

        flush_writer_chk(storage, self.writer)?;

        Ok(LogReceipt {
            start_position: starting_position,
            next_position: self.writer,
        })
    }

    pub fn writer_position(&self) -> u64 {
        self.writer
    }
}

fn flush_writer_chk<S: Storage>(storage: &S, log_pos: u64) -> io::Result<()> {
    storage.write_to(
        FileId::writer_chk(),
        0,
        Bytes::copy_from_slice(log_pos.to_le_bytes().as_slice()),
    )
}
