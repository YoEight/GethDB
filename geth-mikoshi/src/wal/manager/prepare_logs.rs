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
    pub fn next(&mut self) -> io::Result<Option<(u64, PrepareLog)>> {
        if self.log_position >= self.writer {
            return Ok(None);
        }

        if let Some((prepare, log_position, next_pos)) =
            self.inner.read_next_prepare(self.log_position)?
        {
            self.log_position = next_pos;
            return Ok(Some((log_position, prepare)));
        }

        self.log_position = self.writer;

        Ok(None)
    }
}
