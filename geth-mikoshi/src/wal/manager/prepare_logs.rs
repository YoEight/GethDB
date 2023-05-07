use crate::storage::Storage;
use crate::wal::record::PrepareLog;
use crate::wal::ChunkManager;
use std::io;

pub struct PrepareLogs<S> {
    pub(crate) log_position: u64,
    pub(crate) writer: u64,
    pub(crate) inner: ChunkManager<S>,
}

impl<S> PrepareLogs<S>
where
    S: Storage + 'static,
{
    pub fn next(&mut self) -> io::Result<Option<PrepareLog>> {
        if self.log_position >= self.writer {
            return Ok(None);
        }

        let record = self.inner.read_at(self.log_position)?;

        // We advance by the record size plus the pre and post record size (4 bytes each).
        self.log_position += record.size() as u64 + 8;

        Ok(Some(record))
    }
}
