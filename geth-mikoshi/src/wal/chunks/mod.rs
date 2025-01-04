use bytes::{Buf, Bytes, BytesMut};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::{io, mem};

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

struct ContainerInner {
    closed: Vec<Chunk>,
    ongoing: Chunk,
}

#[derive(Clone)]
pub struct ChunkContainer<S> {
    inner: Arc<RwLock<ContainerInner>>,
    storage: S,
}

impl<S> ChunkContainer<S> {
    pub fn load(storage: S) -> io::Result<ChunkContainer<S>>
    where
        S: Storage,
    {
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
            storage.write_to(chunk.file_id(), 0, buffer.freeze())?;

            chunks.push(chunk);
        }

        let ongoing = chunks.pop().unwrap();

        Ok(Self {
            inner: Arc::new(RwLock::new(ContainerInner {
                closed: chunks,
                ongoing,
            })),
            storage,
        })
    }

    pub fn ongoing(&self) -> eyre::Result<Chunk> {
        let inner = self
            .inner
            .read()
            .map_err(|_e| eyre::eyre!("failed to obtained a read-lock on the chunk container"))?;

        Ok(inner.ongoing.clone())
    }

    pub fn find(&self, logical_position: u64) -> eyre::Result<Option<Chunk>> {
        let inner = self
            .inner
            .read()
            .map_err(|_e| eyre::eyre!("failed to obtained a read-lock on the chunk container"))?;

        if inner.ongoing.contains_log_position(logical_position) {
            return Ok(Some(inner.ongoing.clone()));
        }

        for chunk in &inner.closed {
            if chunk.contains_log_position(logical_position) {
                return Ok(Some(chunk.clone()));
            }
        }

        Ok(None)
    }

    pub fn new(&self, buffer: &mut BytesMut, position: u64) -> eyre::Result<Chunk>
    where
        S: Storage,
    {
        let mut inner = self
            .inner
            .write()
            .map_err(|_e| eyre::eyre!("failed to obtained a write-lock on the chunk container"))?;

        let physical_data_size = inner.ongoing.raw_position(position) as usize - CHUNK_HEADER_SIZE;
        let footer = ChunkFooter {
            flags: FooterFlags::IS_COMPLETED,
            physical_data_size,
            logical_data_size: physical_data_size,
            hash: Default::default(),
        };

        footer.put(buffer);
        inner.ongoing.footer = Some(footer);

        self.storage.write_to(
            inner.ongoing.file_id(),
            (CHUNK_SIZE - CHUNK_FOOTER_SIZE) as u64,
            buffer.split().freeze(),
        )?;

        let new_chunk = inner.ongoing.next_chunk();
        let old_chunk = mem::replace(&mut inner.ongoing, new_chunk.clone());

        inner.closed.push(old_chunk);

        Ok(new_chunk)
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }
}

pub struct ChunkBasedWAL<S> {
    buffer: BytesMut,
    container: ChunkContainer<S>,
    writer: u64,
}

impl<S> ChunkBasedWAL<S>
where
    S: Storage + 'static,
{
    pub fn new(container: ChunkContainer<S>) -> io::Result<Self> {
        let mut buffer = BytesMut::new();
        let mut writer = 0u64;
        let storage = container.storage();

        if !storage.exists(FileId::writer_chk())? {
            flush_writer_chk(storage, writer)?;
        } else {
            writer = container
                .storage()
                .read_from(FileId::writer_chk(), 0, 8)?
                .get_u64_le();
        }

        Ok(Self {
            buffer,
            container,
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
    fn append(&mut self, mut entries: &mut LogEntries) -> eyre::Result<LogReceipt> {
        let mut position = self.writer;
        let starting_position = position;
        let storage = self.container.storage();

        let mut chunk = self.container.ongoing()?;
        while let Some(entry) = entries.next() {
            let entry_size = entry.size();
            let projected_next_logical_position = entry_size as u64 + position;

            // Chunk is full, and we need to flush previous data we accumulated. We also create a new
            // chunk for next writes.
            if !chunk.contains_log_position(projected_next_logical_position) {
                let remaining_space = chunk.remaining_space_from(position);
                chunk = self.container.new(&mut self.buffer, position)?;
                position += remaining_space;
            }

            let record = entry.commit(&mut self.buffer, position);
            let local_offset = chunk.raw_position(position);
            position += entry_size as u64;
            storage.write_to(chunk.file_id(), local_offset, record)?;

            self.writer = position;
        }

        flush_writer_chk(storage, self.writer)?;

        Ok(LogReceipt {
            start_position: starting_position,
            next_position: self.writer,
        })
    }

    fn read_at(&self, position: u64) -> eyre::Result<LogEntry> {
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

    fn write_position(&self) -> u64 {
        self.writer
    }
}
