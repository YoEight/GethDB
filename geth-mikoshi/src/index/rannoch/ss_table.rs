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
    pub fn new(bytes: Bytes) -> Self {
        Self(bytes)
    }

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
    pub metas: BlockMetas,
}

impl SsTable {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            metas: BlockMetas(Default::default()),
        }
    }

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

    pub fn find_best_candidates(&self, key: u64, revision: u64) -> Vec<usize> {
        let mut closest_lowest = 0usize;
        let mut closest_highest = 0usize;
        let mut low = 0usize;
        let mut high = self.len() - 1;

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

    pub fn len(&self) -> usize {
        self.metas.len()
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
}
