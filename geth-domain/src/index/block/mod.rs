use std::cmp::Ordering;

pub mod immutable;
pub mod mutable;

pub use immutable::Block;

pub const BLOCK_KEY_SIZE: usize = std::mem::size_of::<u64>();
pub const BLOCK_VERSION_SIZE: usize = std::mem::size_of::<u64>();
pub const BLOCK_LOG_POSITION_SIZE: usize = std::mem::size_of::<u64>();
pub const BLOCK_OFFSET_SIZE: usize = std::mem::size_of::<u64>();
pub const BLOCK_ENTRY_COUNT_SIZE: usize = std::mem::size_of::<u16>();
pub const BLOCK_ENTRY_SIZE: usize = BLOCK_KEY_SIZE + BLOCK_VERSION_SIZE + BLOCK_LOG_POSITION_SIZE;

pub fn get_block_size(count: usize) -> usize {
    count * (BLOCK_ENTRY_SIZE + BLOCK_OFFSET_SIZE) + BLOCK_ENTRY_COUNT_SIZE
}

#[derive(Copy, Clone)]
pub struct KeyId {
    pub key: u64,
    pub revision: u64,
}

impl PartialEq<KeyId> for BlockEntry {
    fn eq(&self, other: &KeyId) -> bool {
        self.key == other.key && self.revision == other.revision
    }
}

impl PartialOrd<KeyId> for BlockEntry {
    fn partial_cmp(&self, other: &KeyId) -> Option<Ordering> {
        let key_ord = self.key.cmp(&other.key);

        if key_ord.is_ne() {
            return Some(key_ord);
        }

        Some(self.revision.cmp(&other.revision))
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub struct BlockEntry {
    pub key: u64,
    pub revision: u64,
    pub position: u64,
}

impl BlockEntry {
    pub fn cmp_key_id(&self, entry: &BlockEntry) -> Ordering {
        self.cmp_key_rev(entry.key, entry.revision)
    }

    pub fn cmp_key_rev(&self, key: u64, revision: u64) -> Ordering {
        let key_ord = self.key.cmp(&key);

        if key_ord.is_ne() {
            return key_ord;
        }

        self.revision.cmp(&revision)
    }
}
