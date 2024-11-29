use bytes::{Buf, Bytes};
use std::cmp::Ordering;

use super::{BlockEntry, BLOCK_ENTRY_SIZE, BLOCK_KEY_SIZE, BLOCK_OFFSET_SIZE};

#[derive(Debug, Clone)]
pub struct Block {
    pub(crate) data: Bytes,
    pub(crate) len: usize,
    pub(crate) offsets: Vec<u16>,
    pub(crate) first_key: Option<u64>,
    pub(crate) last_key: Option<u64>,
}

impl Block {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn from(bytes: Bytes) -> Block {
        let capacity = bytes.len();
        let len = bytes.slice(capacity - 2..).get_u16_le() as usize;
        let mut offset_section =
            bytes.slice((capacity - (len * BLOCK_OFFSET_SIZE + 2))..(capacity - 2));
        let mut offsets = Vec::with_capacity(offset_section.len() / BLOCK_OFFSET_SIZE);

        while offset_section.has_remaining() {
            let offset = offset_section.slice(..BLOCK_OFFSET_SIZE).get_u16_le();
            offsets.push(offset);
            offset_section.advance(BLOCK_OFFSET_SIZE);
        }

        let data = bytes.slice(..len * BLOCK_ENTRY_SIZE);

        let mut first_key = None;
        let mut last_key = None;

        if !offsets.is_empty() {
            let first_entry = offsets.first().copied().unwrap() as usize;
            first_key = Some(
                bytes
                    .slice(first_entry..(first_entry + BLOCK_KEY_SIZE))
                    .get_u64_le(),
            );

            let last_entry = offsets.last().copied().unwrap() as usize;
            last_key = Some(
                bytes
                    .slice(last_entry..(last_entry + BLOCK_KEY_SIZE))
                    .get_u64_le(),
            );
        }

        Block {
            data,
            len,
            offsets,
            first_key,
            last_key,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> Iter<'_> {
        Iter {
            index: 0,
            max: self.len(),
            inner: self,
        }
    }

    pub fn dump(&self) {
        if self.is_empty() {
            println!("<empty_block>");
        }

        for entry in self.iter() {
            println!(
                "key = {}, revision = {}, position = {}",
                entry.key, entry.revision, entry.position,
            );
        }
    }

    pub fn try_read(&self, index: usize) -> Option<BlockEntry> {
        if index > self.len() - 1 {
            return None;
        }

        let offset = self.offsets[index] as usize;
        let mut entry_section = self.data.slice(offset..(offset + BLOCK_ENTRY_SIZE));

        let key = entry_section.get_u64_le();
        let revision = entry_section.get_u64_le();
        let position = entry_section.get_u64_le();

        Some(BlockEntry {
            key,
            revision,
            position,
        })
    }

    pub fn find(&self, key: u64, revision: u64) -> Option<BlockEntry> {
        let mut low = 0usize;
        let mut high = self.len();

        while low <= high {
            let mid = (low + high) / 2;
            let entry = self.try_read(mid)?;

            match entry.cmp_key_rev(key, revision) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid - 1,
                Ordering::Equal => {
                    return Some(entry);
                }
            }
        }

        None
    }

    pub fn scan_forward(&self, key: u64, start: u64, max: usize) -> ScanForward<'_> {
        if let Some(first) = self.first_key {
            if first > key {
                return ScanForward {
                    inner: self,
                    key,
                    start,
                    count: max,
                    max,
                    index: None,
                };
            }
        }

        if self.is_empty() {
            return ScanForward {
                inner: self,
                key,
                start,
                count: max,
                max,
                index: None,
            };
        }

        ScanForward {
            inner: self,
            key,
            start,
            count: 0,
            max,
            index: None,
        }
    }

    pub fn scan_backward(&self, key: u64, start: u64, max: usize) -> ScanBackward<'_> {
        if let Some(last) = self.last_key {
            if last < key {
                return ScanBackward {
                    inner: self,
                    key,
                    start,
                    count: max,
                    max,
                    index: None,
                };
            }
        }

        if self.is_empty() {
            return ScanBackward {
                inner: self,
                key,
                start,
                count: max,
                max,
                index: None,
            };
        }

        ScanBackward {
            inner: self,
            key,
            start,
            count: 0,
            max,
            index: None,
        }
    }
}

pub struct Iter<'a> {
    index: usize,
    max: usize,
    inner: &'a Block,
}

impl<'a> Iterator for Iter<'a> {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index + 1 >= self.max {
            return None;
        }

        let entry = self.inner.try_read(self.index)?;
        self.index += 1;

        Some(entry)
    }
}

pub struct ScanForward<'a> {
    inner: &'a Block,
    key: u64,
    start: u64,
    count: usize,
    max: usize,
    index: Option<usize>,
}

impl<'a> Iterator for ScanForward<'a> {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.count >= self.max {
                return None;
            }

            if self.index.is_none() {
                let mut low = 0usize;
                let mut high = self.inner.len();

                let anchor: Option<usize> = None;
                while low <= high {
                    let mid = (low + high) / 2;
                    let entry = self.inner.try_read(mid)?;

                    match entry.cmp_key_rev(self.key, self.start) {
                        Ordering::Less => low = mid + 1,
                        Ordering::Greater => high = mid - 1,
                        Ordering::Equal => {
                            self.index = Some(mid + 1);
                            return Some(entry);
                        }
                    }
                }

                if anchor.is_none() {
                    self.index = Some(low);
                }
            }

            let current_idx = self.index?;
            let entry = self.inner.try_read(self.index?)?;

            if self.key > entry.key {
                self.index = Some(current_idx + 1);
                continue;
            }

            if self.start > entry.revision {
                self.index = Some(current_idx + 1);
                continue;
            }

            self.count += 1;

            return Some(entry);
        }
    }
}

pub struct ScanBackward<'a> {
    inner: &'a Block,
    key: u64,
    start: u64,
    count: usize,
    max: usize,
    index: Option<usize>,
}

impl<'a> Iterator for ScanBackward<'a> {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.count >= self.max {
                return None;
            }

            if self.index.is_none() {
                let mut low = 0usize;
                let mut high = self.inner.len();

                let anchor: Option<usize> = None;
                while low <= high {
                    let mid = (low + high) / 2;
                    let entry = self.inner.try_read(mid)?;

                    match entry.cmp_key_rev(self.key, self.start) {
                        Ordering::Less => low = mid + 1,
                        Ordering::Greater => high = mid - 1,
                        Ordering::Equal => {
                            self.index = Some(mid + 1);
                            return Some(entry);
                        }
                    }
                }

                if anchor.is_none() {
                    if let Some(index) = low.checked_sub(1) {
                        self.index = Some(index);
                    } else {
                        self.count = self.max;
                        return None;
                    }
                }
            }

            let current_idx = self.index?;
            let entry = self.inner.try_read(self.index?)?;

            if self.key > entry.key {
                self.index = Some(current_idx + 1);
                continue;
            }

            if self.start > entry.revision {
                self.index = Some(current_idx + 1);
                continue;
            }

            self.count += 1;

            return Some(entry);
        }
    }
}
