use crate::backend::esdb::synchronous::fs::{open_chunk_file, read_chunk_record};
use crate::backend::esdb::types::{ChunkBis, PrepareLog, RecordType};
use crate::Entry;
use bytes::BytesMut;
use chrono::{TimeZone, Utc};
use geth_common::Position;
use std::fs::File;
use std::io;
use std::path::PathBuf;

struct WorkItem {
    chunk: ChunkBis,
    file: File,
}

pub struct FullScan {
    buffer: BytesMut,
    end_log_position: u64,
    log_position: u64,
    root: PathBuf,
    chunks: Vec<ChunkBis>,
    work_item: Option<WorkItem>,
}

impl FullScan {
    pub fn new(
        buffer: BytesMut,
        root: PathBuf,
        chunks: Vec<ChunkBis>,
        end_log_position: u64,
    ) -> Self {
        Self {
            buffer,
            end_log_position,
            log_position: 0,
            root,
            chunks,
            work_item: None,
        }
    }

    pub fn next(&mut self) -> io::Result<Option<Entry>> {
        loop {
            if self.log_position == self.end_log_position {
                return Ok(None);
            }

            // We check that the data we are looking for is still in the current chunk. Otherwise,
            // we move to the next one.
            if let Some(work_item) = self.work_item.take() {
                if self.log_position < work_item.chunk.end_position() {
                    self.work_item = Some(work_item);
                }
            }

            // If no current chunk is allocated, we create a new one.
            if self.work_item.is_none() {
                let chunk = self.chunks.remove(0);
                let file =
                    open_chunk_file(&self.root.join(chunk.filename()), &chunk, self.log_position)?;

                self.work_item = Some(WorkItem { chunk, file });
            }

            let work_item = self.work_item.as_mut().unwrap();
            let (header, record) = read_chunk_record(&mut work_item.file, &mut self.buffer)?;
            let next_record_log_position = header.log_position + header.size as u64 + 8;

            self.log_position = next_record_log_position;

            if header.r#type != RecordType::Prepare {
                continue;
            }

            let prepare = PrepareLog::read(1u8, record)?;

            return Ok(Some(Entry {
                id: prepare.event_id,
                r#type: prepare.event_type,
                stream_name: prepare.event_stream_id,
                revision: prepare.expected_version as u64,
                data: prepare.data,
                position: Position(header.log_position),
                created: Utc.timestamp_millis_opt(prepare.timestamp).unwrap(),
            }));
        }
    }
}
