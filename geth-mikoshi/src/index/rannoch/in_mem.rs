use crate::index::rannoch::block::{Block, BlockEntry, BLOCK_ENTRY_SIZE};
use crate::index::rannoch::ss_table::{BlockMetas, SsTable};
use crate::index::rannoch::IndexedPosition;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use nom::character::complete::tab;
use std::collections::{HashMap, VecDeque};
use uuid::Uuid;

pub struct InMemStorage {
    block_size: usize,
    buffer: BytesMut,
    inner: HashMap<Uuid, Bytes>,
}

impl Default for InMemStorage {
    fn default() -> Self {
        InMemStorage::new(4_096)
    }
}

impl InMemStorage {
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            buffer: Default::default(),
            inner: Default::default(),
        }
    }

    pub fn sst_read_block(&self, table: &SsTable, block_idx: usize) -> Option<Block> {
        if block_idx >= table.len() {
            return None;
        }

        let block_meta = table.metas.read(block_idx);
        let mut bytes = self.inner.get(&table.id)?.clone();
        let mut meta_offset_bytes = &bytes[bytes.len() - 4..];
        let meta_offset = meta_offset_bytes.get_u32_le() as usize;
        let len = bytes.len();

        bytes.advance(block_meta.offset as usize);

        let size = if block_idx + 1 >= table.len() {
            meta_offset - block_meta.offset as usize
        } else {
            self.block_size
        };

        Some(Block::new(bytes.copy_to_bytes(size)))
    }

    pub fn sst_find_key(&self, table: &SsTable, key: u64, revision: u64) -> Option<BlockEntry> {
        for block_idx in table.find_best_candidates(key, revision) {
            let block = self.sst_read_block(table, block_idx)?;

            if let Some(entry) = block.find_entry(key, revision) {
                return Some(entry);
            }
        }

        None
    }

    pub fn sst_load(&self, id: Uuid) -> Option<SsTable> {
        let mut bytes = self.inner.get(&id)?.clone();
        let mut footer = &bytes[bytes.len() - 4..];
        let meta_offset = footer.get_u32_le() as usize;

        bytes.advance(meta_offset);
        let metas = bytes.copy_to_bytes(bytes.len() - 4);

        Some(SsTable {
            id,
            metas: BlockMetas::new(metas),
        })
    }

    pub fn sst_put<Values>(&mut self, table: &mut SsTable, mut values: Values)
    where
        Values: IntoIterator<Item = (u64, u64, u64)>,
    {
        let mut table_bytes = self.inner.get(&table.id).cloned().unwrap_or_default();
        let mut offset = 0usize;
        let mut block_current_size = 0usize;

        if !table_bytes.is_empty() {
            let mut footer = &table_bytes[table_bytes.len() - 4..];
            let meta_offset = footer.get_u32_le() as usize;

            offset = meta_offset;
            block_current_size = meta_offset - table.metas.last_block_first_key_offset().unwrap();

            self.buffer.put(table_bytes.copy_to_bytes(meta_offset));
        }

        let mut meta = BytesMut::from(table.metas.as_slice());
        for (key, revision, position) in values {
            if block_current_size + BLOCK_ENTRY_SIZE > self.block_size {
                let remaining = self.block_size - block_current_size;

                self.buffer.put_bytes(0, remaining);
                offset += remaining;
                block_current_size = 0;
            }

            self.buffer.put_u64_le(key);
            self.buffer.put_u64_le(revision);
            self.buffer.put_u64_le(position);

            if block_current_size == 0 {
                meta.put_u32_le(offset as u32);
                meta.put_u64_le(key);
                meta.put_u64_le(revision);
            }

            block_current_size += BLOCK_ENTRY_SIZE;
            offset += BLOCK_ENTRY_SIZE;
        }

        let new_meta = meta.freeze();

        self.buffer.put(new_meta.clone());
        self.buffer.put_u32_le(offset as u32);
        self.inner.insert(table.id, self.buffer.split().freeze());
        table.metas = BlockMetas::new(new_meta);
    }

    pub fn sst_iter(&self, table: &SsTable) -> SsTableIter {
        let block_bytes = if let Some(mut bytes) = self.inner.get(&table.id).cloned() {
            let mut footer = &bytes[bytes.len() - 4..];
            let meta_offset = footer.get_u32_le() as usize;

            bytes.copy_to_bytes(meta_offset)
        } else {
            Default::default()
        };

        SsTableIter {
            block_size: self.block_size,
            block_idx: 0,
            entry_idx: 0,
            block: None,
            block_bytes,
            table: table.clone(),
        }
    }
}

pub struct SsTableIter {
    block_size: usize,
    block_idx: usize,
    entry_idx: usize,
    block: Option<Block>,
    block_bytes: Bytes,
    table: SsTable,
}

impl Iterator for SsTableIter {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.block_bytes.is_empty() && self.block.is_none() {
                return None;
            }

            if self.block_idx >= self.table.len() {
                return None;
            }

            if self.block.is_none() {
                let bytes = if self.block_idx == self.table.len() - 1 {
                    self.block_bytes.copy_to_bytes(self.block_bytes.len())
                } else {
                    self.block_bytes.copy_to_bytes(self.block_size)
                };

                self.block = Some(Block::new(bytes));
            }

            let block = self.block.as_ref()?;

            if self.entry_idx >= block.len() {
                self.block = None;
                self.entry_idx = 0;
                self.block_idx += 1;

                continue;
            }

            let entry = block.read_entry(self.entry_idx)?;
            self.entry_idx += 1;

            return Some(entry);
        }
    }
}
