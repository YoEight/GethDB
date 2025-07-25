use std::mem;

use crate::storage::FileId;
use crate::wal::chunks::ChunkContainer;
use crate::wal::LogEntry;
use bytes::Buf;

use super::chunks::Chunk;

#[derive(Clone)]
pub struct LogReader {
    container: ChunkContainer,
}

impl LogReader {
    pub fn new(container: ChunkContainer) -> Self {
        Self { container }
    }

    pub fn read_at(&self, position: u64) -> eyre::Result<LogEntry> {
        let chunk = if let Some(chunk) = self.container.find(position)? {
            chunk
        } else {
            eyre::bail!("log position {} not found", position);
        };

        self.chunk_read_at(&chunk, position)
    }

    pub fn get_writer_checkpoint(&self) -> eyre::Result<u64> {
        let storage = self.container.storage();
        let mut position = storage.read_from(FileId::writer_chk(), 0, mem::size_of::<u64>())?;

        Ok(position.get_u64_le())
    }

    pub fn entries(&self, start: u64, limit: u64) -> Entries {
        Entries::new(self, start, limit)
    }

    fn chunk_read_at(&self, chunk: &Chunk, position: u64) -> eyre::Result<LogEntry> {
        let storage = self.container.storage();

        let local_offset = chunk.raw_position(position);
        let record_size = storage
            .read_from(chunk.file_id(), local_offset, mem::size_of::<u32>())?
            .get_u32_le() as usize;

        let record_offset = local_offset + mem::size_of::<u32>() as u64;
        let record_bytes = storage.read_from(chunk.file_id(), record_offset, record_size)?;

        let post_record_size_offset = record_offset + record_size as u64;
        let post_record_size = storage
            .read_from(
                chunk.file_id(),
                post_record_size_offset,
                mem::size_of::<u32>(),
            )?
            .get_u32_le() as usize;

        if record_size != post_record_size {
            eyre::bail!(
                "pre and post record size don't match! {} != {}",
                record_size,
                post_record_size
            );
        }

        LogEntry::try_from(record_bytes)
    }
}

pub struct Entries<'a> {
    inner: &'a LogReader,
    current: u64,
    limit: u64,
    chunk: Option<Chunk>,
}

impl<'a> Entries<'a> {
    pub fn new(inner: &'a LogReader, start: u64, limit: u64) -> Self {
        Self {
            inner,
            current: start,
            limit,
            chunk: None,
        }
    }

    pub fn next(&mut self) -> eyre::Result<Option<LogEntry>> {
        loop {
            if self.current >= self.limit {
                return Ok(None);
            }

            if let Some(chunk) = self.chunk.take() {
                if !chunk.contains_log_position(self.current) {
                    continue;
                }

                let entry = self.inner.chunk_read_at(&chunk, self.current)?;
                self.chunk = Some(chunk);
                self.current += (entry.size() + 2 * mem::size_of::<u32>()) as u64;

                return Ok(Some(entry));
            } else if let Some(chunk) = self.inner.container.find(self.current)? {
                self.chunk = Some(chunk);
                continue;
            }

            eyre::bail!("log position {} not found", self.current);
        }
    }
}
