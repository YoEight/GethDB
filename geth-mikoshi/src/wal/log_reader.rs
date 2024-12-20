use crate::storage::Storage;
use crate::wal::chunks::ChunkContainer;
use crate::wal::LogEntry;
use bytes::Buf;

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
