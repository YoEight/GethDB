use crate::index::rannoch::block::{Block, BlockEntry};
use crate::index::rannoch::ss_table::{BlockMetas, SsTable};
use crate::index::rannoch::IndexedPosition;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::{HashMap, VecDeque};
use uuid::Uuid;

pub struct InMemStorage {
    block_size: usize,
    buffer: BytesMut,
    inner: HashMap<Uuid, Bytes>,
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

        let meta = table.metas.read(block_idx);
        let mut bytes = self.inner.get(&table.id)?.clone();
        let len = bytes.len();

        bytes.advance(meta.offset as usize);

        let size = if block_idx + 1 >= table.len() {
            len - meta.offset as usize - 4
        } else {
            let next_meta = table.metas.read(block_idx + 1);
            (next_meta.offset - meta.offset) as usize
        };

        Some(Block::decode(bytes.copy_to_bytes(size)))
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

    pub fn sst_put_single(&mut self, table: &mut SsTable, key: u64, revision: u64, position: u64) {
        self.sst_put(table, key, [IndexedPosition { revision, position }])
    }

    pub fn sst_put<Values>(&mut self, table: &mut SsTable, key: u64, mut values: Values)
    where
        Values: IntoIterator<Item = IndexedPosition>,
    {
        if let Some(bytes) = self.inner.get(&table.id) {
            self.buffer.extend_from_slice(bytes.as_ref());
        }

        let offset = table
            .metas
            .last_block_first_key_offset()
            .unwrap_or_default();

        let mut builder = super::ss_table::Builder {
            count: 0,
            block_size: self.block_size,
            block_builder: super::block::Builder {
                offset,
                buffer: &mut self.buffer,
                count: table.len(),
                block_size: self.block_size,
            },
            metas: BytesMut::from(table.metas.as_slice()),
        };

        for value in values {
            builder.add(key, value.revision, value.position);
        }

        table.metas = builder.close();
        self.inner.insert(table.id, self.buffer.split().freeze());
    }

    pub fn sst_complete(&mut self, table: &SsTable) {
        if let Some(bytes) = self.inner.get_mut(&table.id) {
            self.buffer.extend_from_slice(bytes.as_ref());
            self.buffer.put_u32_le(table.len() as u32);
            *bytes = self.buffer.split().freeze();
        }
    }
}
