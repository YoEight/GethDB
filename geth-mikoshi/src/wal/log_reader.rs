use std::mem;

use crate::storage::Storage;
use crate::wal::chunks::ChunkContainer;
use crate::wal::LogEntry;
use bytes::Buf;

use super::chunks::Chunk;

#[derive(Clone)]
pub struct LogReader<S> {
    container: ChunkContainer<S>,
}

impl<S> LogReader<S> {
    pub fn new(container: ChunkContainer<S>) -> Self {
        Self { container }
    }

    pub fn read_at(&self, position: u64) -> eyre::Result<LogEntry>
    where
        S: Storage,
    {
        let chunk = if let Some(chunk) = self.container.find(position)? {
            chunk
        } else {
            eyre::bail!("log position {} not found", position);
        };

        self.chunk_read_at(&chunk, position)
    }

    pub fn entries(&self, start: u64, limit: u64) -> Entries<S> {
        Entries::new(self, start, limit)
    }

    fn chunk_read_at(&self, chunk: &Chunk, position: u64) -> eyre::Result<LogEntry>
    where
        S: Storage,
    {
        let storage = self.container.storage();

        let local_offset = chunk.raw_position(position);
        let record_size = storage
            .read_from(chunk.file_id(), local_offset, 4)?
            .get_u32_le() as usize;

        let record_bytes = storage.read_from(chunk.file_id(), local_offset + 4, record_size)?;

        let post_record_size = storage
            .read_from(chunk.file_id(), local_offset + 4 + record_size as u64, 4)?
            .get_u32_le() as usize;

        debug_assert_eq!(
            record_size, post_record_size,
            "pre and post record size don't match!"
        );

        Ok(LogEntry::get(record_bytes))
    }
}

pub struct Entries<'a, S> {
    inner: &'a LogReader<S>,
    current: u64,
    limit: u64,
    chunk: Option<Chunk>,
}

impl<'a, S> Entries<'a, S> {
    pub fn new(inner: &'a LogReader<S>, start: u64, limit: u64) -> Self {
        Self {
            inner,
            current: start,
            limit,
            chunk: None,
        }
    }

    pub fn next(&mut self) -> eyre::Result<Option<LogEntry>>
    where
        S: Storage,
    {
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
