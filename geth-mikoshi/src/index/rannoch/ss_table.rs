use crate::index::rannoch::block;
use crate::index::rannoch::block::{Block, BlockEntry};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cmp::Ordering;
use uuid::Uuid;

const SSTABLE_META_ENTRY_SIZE: usize = 4 + 8 + 8;

#[derive(Debug, Clone, Copy)]
pub struct BlockMeta {
    pub offset: u32,
    pub key: u64,
    pub revision: u64,
}

impl BlockMeta {
    pub fn compare_key_id(&self, key: u64, revision: u64) -> Ordering {
        let key_ord = self.key.cmp(&key);

        if key_ord.is_ne() {
            return key_ord;
        }

        self.revision.cmp(&revision)
    }
}

#[derive(Debug, Clone)]
pub struct BlockMetas(Bytes);

impl BlockMetas {
    pub fn read(&self, idx: usize) -> BlockMeta {
        let offset = idx * SSTABLE_META_ENTRY_SIZE;
        let mut bytes = self.0.clone();

        bytes.advance(offset);
        let mut bytes = bytes.copy_to_bytes(SSTABLE_META_ENTRY_SIZE);

        BlockMeta {
            offset: bytes.get_u32_le(),
            key: bytes.get_u64_le(),
            revision: bytes.get_u64_le(),
        }
    }

    pub fn len(&self) -> usize {
        self.0.len() / SSTABLE_META_ENTRY_SIZE
    }

    pub fn last_block_first_key_offset(&self) -> Option<usize> {
        if self.len() == 0 {
            return None;
        }

        Some(self.read(self.len() - 1).offset as usize)
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct SsTable {
    pub id: Uuid,
    data: Bytes,
    pub metas: BlockMetas,
    pub count: usize,
}

impl SsTable {
    pub fn builder(buffer: &mut BytesMut, block_size: usize) -> Builder {
        let metas = buffer.split();
        let block = Block::builder(buffer, block_size);

        Builder {
            metas,
            count: 0,
            block_size,
            block_builder: block,
        }
    }

    pub fn read_block(&self, block_idx: usize) -> Option<Block> {
        if block_idx >= self.count {
            return None;
        }

        let meta = self.metas.read(block_idx);
        let mut bytes = self.data.clone();
        bytes.advance(meta.offset as usize);

        let size = if block_idx + 1 >= self.count {
            self.data.len() - meta.offset as usize - 4
        } else {
            let next_meta = self.metas.read(block_idx + 1);
            (next_meta.offset - meta.offset) as usize
        };

        let block_bytes = bytes.copy_to_bytes(size);

        Some(Block::decode(block_bytes))
    }

    pub fn find_best_candidates(&self, key: u64, revision: u64) -> Vec<usize> {
        let mut closest_lowest = 0usize;
        let mut closest_highest = 0usize;
        let mut low = 0usize;
        let mut high = self.count - 1;

        while low <= high {
            let mid = (low + high) / 2;
            let meta = self.metas.read(mid);

            match meta.compare_key_id(key, revision) {
                Ordering::Less => {
                    closest_lowest = mid;
                    low = mid + 1;
                }

                Ordering::Greater => {
                    closest_highest = mid;
                    high = mid - 1;
                }

                Ordering::Equal => return vec![mid],
            }
        }

        vec![closest_lowest, closest_highest]
    }

    pub fn find_key(&self, key: u64, revision: u64) -> Option<BlockEntry> {
        for block_id in self.find_best_candidates(key, revision) {
            let block = self.read_block(block_id)?;

            if let Some(entry) = block.find_entry(key, revision) {
                return Some(entry);
            }
        }

        None
    }

    pub fn encode(&self, buffer: &mut BytesMut) {
        buffer.put(self.data.clone());
        let offset = buffer.len();
        buffer.put(self.metas.0.clone());
        buffer.put_u32_le(offset as u32);
    }

    pub fn decode(id: Uuid, mut bytes: Bytes) -> Self {
        let mut metas_offset = &bytes[bytes.len() - 4..];
        let metas_offset = metas_offset.get_u32_le() as usize;
        let data = bytes.copy_to_bytes(metas_offset);
        let metas = bytes.copy_to_bytes(bytes.len() - 4);
        let count = metas.len() / SSTABLE_META_ENTRY_SIZE;

        Self {
            id,
            data,
            metas: BlockMetas(metas),
            count,
        }
    }

    pub fn iter(&self) -> Iter {
        Iter {
            block_idx: 0,
            entry_idx: 0,
            block: None,
            table: self.clone(),
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }
}

pub struct Builder<'a> {
    pub count: usize,
    pub block_size: usize,
    pub block_builder: block::Builder<'a>,
    pub metas: BytesMut,
}

impl<'a> Builder<'a> {
    pub fn add(&mut self, key: u64, revision: u64, position: u64) {
        let mut attempts = 1;

        loop {
            if self.block_builder.add(key, revision, position) {
                if self.block_builder.count() == 1 {
                    self.count += 1;
                    self.metas.put_u32_le(self.block_builder.offset() as u32);
                    self.metas.put_u64_le(key);
                    self.metas.put_u64_le(revision);
                }

                return;
            }

            if attempts >= 2 {
                panic!("Failed to push new entry even after creating a new block. Block size must be too short");
            }

            self.block_builder.new_block();
            attempts += 1;
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn build(self) -> SsTable {
        let mut buffer = self.block_builder.complete();

        buffer.put_u32_le(self.count as u32);

        SsTable {
            id: Uuid::new_v4(),
            data: buffer.split().freeze(),
            metas: BlockMetas(self.metas.freeze()),
            count: self.count,
        }
    }

    pub fn close(self) -> BlockMetas {
        BlockMetas(self.metas.freeze())
    }
}

pub struct Iter {
    block_idx: usize,
    entry_idx: usize,
    block: Option<Block>,
    table: SsTable,
}

impl Iterator for Iter {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.block_idx >= self.table.len() {
                return None;
            }

            if self.block.is_none() {
                self.block = Some(self.table.read_block(self.block_idx)?);
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
