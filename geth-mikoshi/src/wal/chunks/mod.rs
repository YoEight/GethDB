use crate::constants::{CHUNK_FOOTER_SIZE, CHUNK_HEADER_SIZE, CHUNK_SIZE};
use crate::storage::{FileId, Storage};
use crate::wal::chunks::chunk::{Chunk, ChunkInfo};
use crate::wal::chunks::footer::{ChunkFooter, FooterFlags};
use crate::wal::chunks::header::ChunkHeader;
use crate::wal::chunks::manager::Chunks;
use crate::wal::{LogEntry, LogEntryType, LogReceipt, LogRecord, WriteAheadLog};
use bytes::{Buf, Bytes, BytesMut};
use std::collections::BTreeMap;
use std::io;

mod chunk;
mod footer;
mod header;
pub mod manager;
pub mod record;

pub struct ChunkBasedWAL<S> {
    buffer: BytesMut,
    storage: S,
    chunks: Vec<Chunk>,
    writer: u64,
}

impl<S> ChunkBasedWAL<S> {
    fn ongoing_chunk(&self) -> Chunk {
        self.chunks.last().cloned().unwrap()
    }

    fn new_chunk(&mut self) -> Chunk {
        let new = self.ongoing_chunk().next_chunk();
        self.chunks.push(new.clone());

        new
    }

    fn chunk_mut(&mut self, idx: usize) -> &mut Chunk {
        &mut self.chunks[idx]
    }

    fn ongoing_chunk_mut(&mut self) -> &mut Chunk {
        self.chunk_mut(self.chunks.len() - 1)
    }

    fn find_chunk(&self, logical_position: u64) -> Option<&Chunk> {
        for chunk in &self.chunks {
            if chunk.contains_log_position(logical_position) {
                return Some(chunk);
            }
        }

        None
    }
}

impl<S> ChunkBasedWAL<S>
where
    S: Storage + 'static,
{
    pub fn load(storage: S) -> io::Result<Self> {
        let mut buffer = BytesMut::new();
        let mut sorted_chunks = BTreeMap::<usize, ChunkInfo>::new();

        for info in storage.list(Chunks)? {
            if let Some(chunk) = sorted_chunks.get_mut(&info.seq_num) {
                if chunk.version < info.version {
                    *chunk = info;
                }
            } else {
                sorted_chunks.insert(info.seq_num, info);
            }
        }

        let mut chunks = Vec::new();
        for info in sorted_chunks.into_values() {
            let header = storage.read_from(info.file_id(), 0, CHUNK_HEADER_SIZE)?;
            let header = ChunkHeader::get(header);
            let footer = storage.read_from(
                info.file_id(),
                (CHUNK_SIZE - CHUNK_FOOTER_SIZE) as u64,
                CHUNK_FOOTER_SIZE,
            )?;
            let footer = ChunkFooter::get(footer);
            let chunk = Chunk {
                info,
                header,
                footer,
            };

            chunks.push(chunk);
        }

        if chunks.is_empty() {
            let chunk = Chunk::new(0);

            chunk.header.put(&mut buffer);
            storage.write_to(chunk.file_id(), 0, buffer.split().freeze())?;

            chunks.push(chunk);
        }

        let mut writer = 0u64;
        if !storage.exists(FileId::writer_chk())? {
            flush_writer_chk(&storage, writer)?;
        } else {
            writer = storage.read_from(FileId::writer_chk(), 0, 8)?.get_u64_le();
        }

        Ok(Self {
            buffer,
            storage,
            chunks,
            writer,
        })
    }
}

fn flush_writer_chk<S: Storage>(storage: &S, log_pos: u64) -> io::Result<()> {
    storage.write_to(
        FileId::writer_chk(),
        0,
        Bytes::copy_from_slice(log_pos.to_le_bytes().as_slice()),
    )
}

impl<S> WriteAheadLog for ChunkBasedWAL<S>
where
    S: Storage + 'static,
{
    fn append<A: LogRecord>(&mut self, record: A) -> io::Result<LogReceipt> {
        let mut starting_position = self.writer;
        let r#type = A::r#type();

        record.put(&mut self.buffer);

        let mut entry = LogEntry {
            position: starting_position,
            r#type,
            payload: self.buffer.split().freeze(),
        };

        let mut chunk: Chunk = self.ongoing_chunk();
        let projected_next_logical_position = entry.size() + starting_position;

        // Chunk is full and we need to flush previous data we accumulated. We also create a new
        // chunk for next writes.
        if !chunk.contains_log_position(projected_next_logical_position) {
            let physical_data_size =
                chunk.raw_position(starting_position) as usize - CHUNK_HEADER_SIZE;
            let footer = ChunkFooter {
                flags: FooterFlags::IS_COMPLETED,
                physical_data_size,
                logical_data_size: physical_data_size,
                hash: Default::default(),
            };

            footer.put(&mut self.buffer);
            self.ongoing_chunk_mut().footer = Some(footer);

            self.storage.write_to(
                chunk.file_id(),
                (CHUNK_SIZE - CHUNK_FOOTER_SIZE) as u64,
                self.buffer.split().freeze(),
            )?;

            starting_position += chunk.remaining_space_from(starting_position);
            chunk = self.new_chunk();
        }

        entry.position = starting_position;
        let next_logical_position = entry.size() as u64 + starting_position;
        let local_offset = chunk.raw_position(starting_position);
        entry.put(&mut self.buffer);
        self.storage
            .write_to(chunk.file_id(), local_offset, self.buffer.split().freeze())?;

        self.writer = next_logical_position;
        flush_writer_chk(&self.storage, self.writer)?;

        Ok(LogReceipt {
            position: starting_position,
            next_position: next_logical_position,
        })
    }

    fn read_at(&mut self, position: u64) -> io::Result<LogEntry> {
        let chunk = if let Some(chunk) = self.find_chunk(position) {
            chunk
        } else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("log position {} doesn't exist", position),
            ));
        };

        let local_offset = chunk.raw_position(position);
        let record_size = self
            .storage
            .read_from(chunk.file_id(), local_offset, 4)?
            .get_u32_le() as usize;

        let mut record_bytes =
            self.storage
                .read_from(chunk.file_id(), local_offset + 4, record_size)?;

        let r#type = LogEntryType::from_raw(record_bytes.get_u8());

        Ok(LogEntry {
            position,
            r#type,
            payload: record_bytes,
        })
    }
}