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

    pub fn find_best_candidates(&self, key: u64, revision: u64) -> Vec<usize> {
        let mut closest_lowest = 0usize;
        let mut closest_highest = 0usize;
        let mut low = 0i64;
        let mut high = (self.len() - 1) as i64;

        while low <= high {
            let mid = (low + high) / 2;
            let meta = self.metas.read(mid as usize);

            match meta.compare_key_id(key, revision) {
                Ordering::Less => {
                    closest_lowest = mid as usize;
                    low = mid + 1;
                }

                Ordering::Greater => {
                    closest_highest = mid as usize;
                    high = mid - 1;
                }

                Ordering::Equal => return vec![mid as usize],
            }
        }

        vec![closest_lowest, closest_highest]
    }

    pub fn len(&self) -> usize {
        self.metas.len()
    }
}
