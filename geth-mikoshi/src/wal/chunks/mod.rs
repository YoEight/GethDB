use std::collections::BTreeMap;
use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::constants::{CHUNK_FOOTER_SIZE, CHUNK_HEADER_SIZE, CHUNK_SIZE};
use crate::storage::{FileCategory, FileId, Storage};
use crate::wal::chunks::chunk::{Chunk, ChunkInfo};
use crate::wal::chunks::footer::{ChunkFooter, FooterFlags};
use crate::wal::chunks::header::ChunkHeader;
use crate::wal::{LogEntries, LogEntry, LogReceipt, WriteAheadLog};

mod chunk;
mod footer;
mod header;

#[cfg(test)]
mod tests;

#[derive(Copy, Clone, Debug)]
pub struct Chunks;
impl FileCategory for Chunks {
    type Item = ChunkInfo;

    fn parse(&self, name: &str) -> Option<Self::Item> {
        ChunkInfo::from_chunk_filename(name)
    }
}

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
        self.chunks
            .iter()
            .find(|&chunk| chunk.contains_log_position(logical_position))
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
    fn append(&mut self, mut entries: LogEntries) -> io::Result<LogReceipt> {
        let mut position = self.writer;
        let starting_position = position;

        while let Some(entry) = entries.next() {
            let entry_size = entry.size();
            let mut chunk: Chunk = self.ongoing_chunk();
            let projected_next_logical_position = entry_size as u64 + position;

            // Chunk is full, and we need to flush previous data we accumulated. We also create a new
            // chunk for next writes.
            if !chunk.contains_log_position(projected_next_logical_position) {
                let physical_data_size = chunk.raw_position(position) as usize - CHUNK_HEADER_SIZE;
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

                position += chunk.remaining_space_from(position);
                chunk = self.new_chunk();
            }

            let record = entry.commit(&mut self.buffer, position);
            let local_offset = chunk.raw_position(position);
            position += entry_size as u64;
            self.storage
                .write_to(chunk.file_id(), local_offset, record)?;

            self.writer = position;
        }

        flush_writer_chk(&self.storage, self.writer)?;

        Ok(LogReceipt {
            start_position: starting_position,
            next_position: self.writer,
        })
    }

    fn read_at(&self, position: u64) -> io::Result<LogEntry> {
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

        let record_bytes =
            self.storage
                .read_from(chunk.file_id(), local_offset + 4, record_size)?;

        let post_record_size = self
            .storage
            .read_from(chunk.file_id(), local_offset + 4 + record_size as u64, 4)?
            .get_u32_le() as usize;

        assert_eq!(
            record_size, post_record_size,
            "pre and post record size don't match!"
        );

        Ok(LogEntry::get(record_bytes))
    }

    fn write_position(&self) -> u64 {
        self.writer
    }
}
