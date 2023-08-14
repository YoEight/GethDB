use crate::domain::StreamEventAppended;
use crate::wal::{LogEntryType, WALRef, WriteAheadLog};
use crate::IteratorIO;
use std::io;

pub struct DataEvents<WAL> {
    pub(crate) log_position: u64,
    pub(crate) max_position: u64,
    pub(crate) wal: WALRef<WAL>,
}

impl<WAL: WriteAheadLog> DataEvents<WAL> {
    pub fn new(wal: WALRef<WAL>, from: u64, to: u64) -> Self {
        Self {
            log_position: from,
            max_position: to,
            wal,
        }
    }
}

impl<WAL> IteratorIO for DataEvents<WAL>
where
    WAL: WriteAheadLog,
{
    type Item = (u64, StreamEventAppended);

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        loop {
            if self.log_position >= self.max_position {
                return Ok(None);
            }

            let record = self.wal.read_at(self.log_position)?;
            self.log_position += record.size() as u64;

            if record.r#type != LogEntryType::UserData {
                continue;
            }

            return Ok(Some((record.position, record.unmarshall())));
        }
    }
}
