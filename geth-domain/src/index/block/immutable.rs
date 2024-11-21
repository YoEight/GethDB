use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cmp::Ordering;

use geth_common::{Direction, Revision};

use super::{BlockEntry, KeyId, BLOCK_ENTRY_SIZE};

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
        let count = src.slice(src.len() - 2..).get_u16_le();

        Self {
            data: src.copy_to_bytes(count as usize * BLOCK_ENTRY_SIZE),
        }
    }

    pub fn dump(&self) {
        if self.count() == 0 {
            println!("<empty_block>");
        }

        let mut temp = self.data.as_ref();

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
        let mut temp = &self.data[offset..offset + BLOCK_ENTRY_SIZE];

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

    pub fn scan(&self, key: u64, direction: Direction, start: Revision<u64>, count: usize) -> Scan {
        Scan::new(key, self.data.clone(), direction, start, count)
    }

    pub fn len(&self) -> usize {
        self.count()
    }
}

fn read_block_entry(bytes: &[u8], idx: usize) -> Option<BlockEntry> {
    if bytes.remaining() < BLOCK_ENTRY_SIZE {
        return None;
    }

    let offset = idx * BLOCK_ENTRY_SIZE;
    let mut temp = &bytes[offset..offset + BLOCK_ENTRY_SIZE];

    Some(BlockEntry {
        key: temp.get_u64_le(),
        revision: temp.get_u64_le(),
        position: temp.get_u64_le(),
    })
}

fn read_block_entry_mut(mut bytes: &[u8]) -> BlockEntry {
    BlockEntry {
        key: bytes.get_u64_le(),
        revision: bytes.get_u64_le(),
        position: bytes.get_u64_le(),
    }
}

fn block_entry_len(bytes: &Bytes) -> usize {
    bytes.len() / BLOCK_ENTRY_SIZE
}

enum SearchResult {
    Found { index: usize, entry: BlockEntry },
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
                    index: mid as usize,
                    entry,
                }
            }
        }
    }

    SearchResult::NotFound { edge: low as usize }
}

pub struct Scan {
    key: u64,
    buffer: Bytes,
    anchored: bool,
    revision: u64,
    index: usize,
    direction: Direction,
    len: usize,
    count: usize,
}

impl Scan {
    fn new(
        key: u64,
        buffer: Bytes,
        direction: Direction,
        start: Revision<u64>,
        mut count: usize,
    ) -> Self {
        let revision = match start {
            Revision::Start => 0,
            Revision::End => u64::MAX,
            Revision::Revision(r) => r,
        };

        let len = buffer.len() / BLOCK_ENTRY_SIZE;

        if len != 0 {
            let first_entry = read_block_entry(&buffer, 0).unwrap();
            let last_entry = read_block_entry(&buffer, len - 1).unwrap();

            if first_entry.key > key || last_entry.key < key {
                count = 0;
            }
        } else {
            count = 0;
        }

        Self {
            key,
            direction,
            revision,
            buffer,
            count,
            len,
            index: 0,
            anchored: false,
        }
    }
}

impl Scan {
    fn progress(&mut self) {
        self.count -= 1;

        match self.direction {
            Direction::Forward => {
                self.index += 1;

                if self.index >= self.len {
                    self.count = 0;
                }
            }

            Direction::Backward => {
                if let Some(value) = self.index.checked_sub(1) {
                    self.index = value;
                    return;
                }

                self.count = 0;
            }
        }
    }
}

impl Iterator for Scan {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.count == 0 {
                return None;
            }

            if !self.anchored {
                self.anchored = true;
                match find_block_entry(&self.buffer, self.key, self.revision) {
                    SearchResult::Found { index, entry } => {
                        self.index = index;
                        self.progress();

                        return Some(entry);
                    }

                    SearchResult::NotFound { edge } => {
                        self.index = edge;

                        if self.direction == Direction::Backward {
                            if let Some(value) = self.index.checked_sub(1) {
                                self.index = value;
                            } else {
                                self.count = 0;
                            }
                        }

                        continue;
                    }
                }
            }

            let offset = self.index * BLOCK_ENTRY_SIZE;
            let temp = &self.buffer[offset..offset + BLOCK_ENTRY_SIZE];
            let entry = read_block_entry_mut(temp);

            if entry.key != self.key {
                self.count = 0;

                return None;
            }

            self.progress();

            return Some(entry);
        }
    }
}
