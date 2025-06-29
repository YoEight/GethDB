use bytes::BytesMut;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::{io, mem};

use crate::constants::{CHUNK_FOOTER_SIZE, CHUNK_HEADER_SIZE, CHUNK_SIZE};
use crate::storage::{FileCategory, Storage};
use crate::wal::chunks::chunk::ChunkInfo;
use crate::wal::chunks::footer::{ChunkFooter, FooterFlags};
use crate::wal::chunks::header::ChunkHeader;

mod chunk;
mod footer;
mod header;

#[cfg(test)]
mod tests;

pub use chunk::Chunk;

#[derive(Copy, Clone, Debug)]
pub struct Chunks;
impl FileCategory for Chunks {
    type Item = ChunkInfo;

    fn parse(&self, name: &str) -> Option<Self::Item> {
        ChunkInfo::from_chunk_filename(name)
    }
}

#[derive(Debug)]
struct ContainerInner {
    closed: Vec<Chunk>,
    ongoing: Chunk,
}

#[derive(Debug, Clone)]
pub struct ChunkContainer {
    inner: Arc<RwLock<ContainerInner>>,
    storage: Storage,
}

impl ChunkContainer {
    pub fn load(storage: Storage) -> io::Result<ChunkContainer> {
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

    pub fn new_chunk(&self, buffer: &mut BytesMut, position: u64) -> eyre::Result<Chunk> {
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

    pub fn storage(&self) -> &Storage {
        &self.storage
    }
}
