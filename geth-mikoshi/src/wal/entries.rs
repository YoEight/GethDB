use std::io;

use crate::IteratorIO;
use crate::wal::{LogEntry, WALRef, WriteAheadLog};

pub struct EntryIter<WAL> {
    pub(crate) log_position: u64,
    pub(crate) max_position: u64,
    pub(crate) wal: WALRef<WAL>,
}

impl<WAL: WriteAheadLog> EntryIter<WAL> {
    pub fn new(wal: WALRef<WAL>, from: u64, to: u64) -> Self {
        Self {
            log_position: from,
            max_position: to,
            wal,
        }
    }
}

impl<WAL> IteratorIO for EntryIter<WAL>
where
    WAL: WriteAheadLog,
{
    type Item = LogEntry;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        if self.log_position >= self.max_position {
            return Ok(None);
        }

        let entry = self.wal.read_at(self.log_position)?;
        self.log_position += entry.size() as u64;

        Ok(Some(entry))
    }
}
