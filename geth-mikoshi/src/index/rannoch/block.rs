#[cfg(test)]
mod tests;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cmp::Ordering;
use std::collections::btree_map::Entry;

struct Offsets(Bytes);

pub const BLOCK_KEY_SIZE: usize = 8;
pub const BLOCK_VERSION_SIZE: usize = 8;
pub const BLOCK_LOG_POSITION_SIZE: usize = 8;
pub const BLOCK_ENTRY_SIZE: usize = BLOCK_KEY_SIZE + BLOCK_VERSION_SIZE + BLOCK_LOG_POSITION_SIZE;
pub const BLOCK_MIN_SIZE: usize = BLOCK_ENTRY_SIZE + 2;

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

#[derive(Debug, Copy, Clone)]
pub struct BlockEntry {
    pub key: u64,
    pub revision: u64,
    pub position: u64,
}

#[derive(Debug, Clone)]
pub struct Block {
    data: Bytes,
    count: usize,
}

impl Block {
    pub fn builder(buffer: &mut BytesMut, block_size: usize) -> Builder {
        Builder {
            offset: buffer.len(),
            buffer,
            count: 0,
            block_size,
        }
    }

    pub fn encode(&self, buffer: &mut BytesMut) -> Bytes {
        buffer.put(self.data.clone());
        buffer.put_u16_le(self.count as u16);

        buffer.split().freeze()
    }

    pub fn decode(mut src: Bytes) -> Block {
        let mut count = &src[src.len() - 2..];
        let count = count.get_u16_le();

        Self {
            data: src.copy_to_bytes(count as usize * BLOCK_ENTRY_SIZE),
            count: count as usize,
        }
    }

    pub fn dump(&self) {
        let mut temp = self.data.clone();

        for _ in 0..self.count {
            println!(
                "key = {}, revision = {}, position = {}",
                temp.get_u64_le(),
                temp.get_u64_le(),
                temp.get_u64_le()
            );
        }
    }

    pub fn read_entry(&self, idx: usize) -> Option<BlockEntry> {
        if idx >= self.count {
            return None;
        }

        let offset = idx * BLOCK_ENTRY_SIZE;
        let mut temp = self.data.clone();
        temp.advance(offset);

        let entry = BlockEntry {
            key: temp.get_u64_le(),
            revision: temp.get_u64_le(),
            position: temp.get_u64_le(),
        };

        Some(entry)
    }

    pub fn find_entry(&self, key: u64, revision: u64) -> Option<BlockEntry> {
        let key_id = KeyId { key, revision };
        let mut low = 0usize;
        let mut high = self.count - 1;

        while low <= high {
            let mid = (low + high) / 2;
            let entry = self.read_entry(mid)?;

            match entry.partial_cmp(&key_id)? {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid - 1,
                Ordering::Equal => return Some(entry),
            }
        }

        None
    }
}

pub struct Builder<'a> {
    offset: usize,
    buffer: &'a mut BytesMut,
    count: usize,
    block_size: usize,
}

impl<'a> Builder<'a> {
    pub fn add(&mut self, key: u64, revision: u64, position: u64) -> bool {
        if self.size() + BLOCK_ENTRY_SIZE > self.block_size {
            return false;
        }

        self.buffer.put_u64_le(key);
        self.buffer.put_u64_le(revision);
        self.buffer.put_u64_le(position);
        self.count += 1;

        true
    }

    pub fn build(self) -> Block {
        Block {
            data: self.buffer.split().freeze(),
            count: self.count,
        }
    }

    pub fn new_block(&mut self) {
        self.buffer.put_u16_le(self.count as u16);
        self.count = 0;
        self.offset = self.buffer.len();
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn complete(mut self) -> &'a mut BytesMut {
        self.buffer.put_u16_le(self.count as u16);
        self.buffer
    }

    pub fn size(&self) -> usize {
        self.buffer.len() - self.offset
    }
}