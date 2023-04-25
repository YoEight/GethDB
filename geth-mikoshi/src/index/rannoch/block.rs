use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::Bound;
use std::fs::read;
use std::ops::RangeBounds;

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

#[derive(Debug, Clone)]
pub struct Block {
    data: Bytes,
}

impl Block {
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }

    pub fn count(&self) -> usize {
        self.data.len() / BLOCK_ENTRY_SIZE
    }

    pub fn encode(&self, buffer: &mut BytesMut) -> Bytes {
        buffer.put(self.data.clone());
        buffer.put_u16_le(self.count() as u16);

        buffer.split().freeze()
    }

    pub fn decode(mut src: Bytes) -> Block {
        let mut count = &src[src.len() - 2..];
        let count = count.get_u16_le();

        Self {
            data: src.copy_to_bytes(count as usize * BLOCK_ENTRY_SIZE),
        }
    }

    pub fn dump(&self) {
        if self.count() == 0 {
            println!("<empty_block>");
        }

        let mut temp = self.data.clone();

        for _ in 0..self.count() {
            println!(
                "key = {}, revision = {}, position = {}",
                temp.get_u64_le(),
                temp.get_u64_le(),
                temp.get_u64_le()
            );
        }
    }

    pub fn read_entry(&self, idx: usize) -> Option<BlockEntry> {
        if idx >= self.count() {
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
        find_block_entry(&self.data, key, revision).some()
    }

    pub fn scan<R>(&self, key: u64, range: R) -> Scan<R>
    where
        R: RangeBounds<u64>,
    {
        Scan::new(key, self.data.clone(), range)
    }

    pub fn len(&self) -> usize {
        self.count()
    }
}

fn read_block_entry(bytes: &Bytes, idx: usize) -> Option<BlockEntry> {
    if bytes.remaining() < BLOCK_ENTRY_SIZE {
        return None;
    }

    let mut temp = bytes.clone();
    temp.advance(idx * BLOCK_ENTRY_SIZE);

    Some(BlockEntry {
        key: temp.get_u64_le(),
        revision: temp.get_u64_le(),
        position: temp.get_u64_le(),
    })
}

fn read_block_entry_mut(bytes: &mut Bytes) -> Option<BlockEntry> {
    if bytes.remaining() < BLOCK_ENTRY_SIZE {
        return None;
    }

    Some(BlockEntry {
        key: bytes.get_u64_le(),
        revision: bytes.get_u64_le(),
        position: bytes.get_u64_le(),
    })
}

fn block_entry_len(bytes: &Bytes) -> usize {
    bytes.len() / BLOCK_ENTRY_SIZE
}

enum SearchResult {
    Found { offset: usize, entry: BlockEntry },
    NotFound { edge: usize },
}

impl SearchResult {
    fn some(self) -> Option<BlockEntry> {
        match self {
            SearchResult::Found { entry, .. } => Some(entry),
            SearchResult::NotFound { .. } => None,
        }
    }
}

fn find_block_entry(bytes: &Bytes, key: u64, revision: u64) -> SearchResult {
    let key_id = KeyId { key, revision };
    let mut low = 0i64;
    let mut high = (block_entry_len(bytes) - 1) as i64;

    while low <= high {
        let mid = (low + high) / 2;
        let entry = read_block_entry(bytes, mid as usize).unwrap();

        match entry.partial_cmp(&key_id).unwrap() {
            Ordering::Less => low = mid + 1,
            Ordering::Greater => high = mid - 1,
            Ordering::Equal => {
                return SearchResult::Found {
                    offset: mid as usize,
                    entry,
                }
            }
        }
    }

    SearchResult::NotFound { edge: low as usize }
}

pub struct Scan<R> {
    key: u64,
    revision: u64,
    buffer: Bytes,
    range: R,
    anchored: bool,
}

impl<R> Scan<R>
where
    R: RangeBounds<u64>,
{
    fn new(key: u64, mut buffer: Bytes, range: R) -> Self {
        let current = match range.start_bound() {
            Bound::Included(x) => *x,
            Bound::Excluded(x) => *x + 1,
            Bound::Unbounded => 0,
        };

        let count = buffer.len() / BLOCK_ENTRY_SIZE;
        if count != 0 {
            let first_entry = read_block_entry(&buffer, 0).unwrap();
            let last_entry = read_block_entry(&buffer, count - 1).unwrap();

            if first_entry.key > key || last_entry.key < key {
                buffer = Bytes::new();
            }
        } else {
            buffer = Bytes::new();
        }

        Self {
            key,
            revision: current,
            buffer,
            range,
            anchored: false,
        }
    }
}

impl<R> Iterator for Scan<R>
where
    R: RangeBounds<u64>,
{
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !self.buffer.has_remaining() || self.buffer.len() / BLOCK_ENTRY_SIZE == 0 {
                return None;
            }

            match self.range.end_bound() {
                Bound::Included(end) | Bound::Excluded(end) if self.revision > *end => {
                    self.buffer = Bytes::new();
                    return None;
                }
                _ => {}
            }

            if !self.anchored {
                self.anchored = true;
                match find_block_entry(&self.buffer, self.key, self.revision) {
                    SearchResult::Found { offset, entry } => {
                        // We move pass the entry we are about to return.
                        self.buffer
                            .advance(offset * BLOCK_ENTRY_SIZE + BLOCK_ENTRY_SIZE);
                        self.revision += 1;

                        return Some(entry);
                    }

                    SearchResult::NotFound { edge } => {
                        self.buffer.advance(edge * BLOCK_ENTRY_SIZE);

                        continue;
                    }
                }
            }

            if let Some(entry) = read_block_entry_mut(&mut self.buffer) {
                if entry.key != self.key {
                    self.buffer = Bytes::new();
                    return None;
                }

                self.revision = entry.revision + 1;
                return Some(entry);
            }
        }
    }
}
