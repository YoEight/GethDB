use std::{cmp::Ordering, sync::Arc};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::index::block::BLOCK_ENTRY_COUNT_SIZE;

use super::{
    get_block_size, Block, BlockEntry, BLOCK_ENTRY_SIZE, BLOCK_KEY_SIZE, BLOCK_OFFSET_SIZE,
};

pub struct BlockMut {
    data: BytesMut,
    capacity: usize,
    len: usize,
    offsets: Vec<u16>,
    first_key: Option<u64>,
    last_key: Option<u64>,
}

impl Default for BlockMut {
    fn default() -> Self {
        Self::new(4_096)
    }
}

impl BlockMut {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: BytesMut::new(),
            capacity,
            len: 0,
            offsets: vec![],
            first_key: None,
            last_key: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn try_from(mut data: BytesMut, bytes: Bytes) -> Option<Self> {
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

        data.put(bytes.slice(..len * BLOCK_ENTRY_SIZE));

        let mut first_key = None;
        let mut last_key = None;

        if !offsets.is_empty() {
            let first_entry = offsets.first().copied()? as usize;
            first_key = Some(
                bytes
                    .slice(first_entry..(first_entry + BLOCK_KEY_SIZE))
                    .get_u64_le(),
            );

            let last_entry = offsets.last().copied()? as usize;
            last_key = Some(
                bytes
                    .slice(last_entry..(last_entry + BLOCK_KEY_SIZE))
                    .get_u64_le(),
            );
        }

        Some(Self {
            data,
            capacity,
            len,
            offsets,
            first_key,
            last_key,
        })
    }

    pub fn try_add(&mut self, key: u64, revision: u64, position: u64) -> bool {
        if self.estimated_size() + BLOCK_ENTRY_SIZE + BLOCK_OFFSET_SIZE > self.capacity {
            return false;
        }

        let offset = self.data.len() as u16;
        self.data.put_u64_le(key);
        self.data.put_u64_le(revision);
        self.data.put_u64_le(position);
        self.offsets.push(offset);

        self.len += 1;

        true
    }

    pub fn try_read(&self, index: usize) -> Option<BlockEntry> {
        if index > self.len() - 1 {
            return None;
        }

        let offset = self.offsets[index] as usize;
        let mut entry_section = &self.data.as_ref()[offset..(offset + BLOCK_ENTRY_SIZE)];

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

    pub fn estimated_size(&self) -> usize {
        get_block_size(self.len)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> Iter<'_> {
        Iter {
            index: 0,
            max: self.len(),
            inner: self,
        }
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

    pub fn freeze(mut self) -> Block {
        let entries_end = self.len() * BLOCK_ENTRY_SIZE;
        let offset_section_start =
            self.capacity - (self.len() * BLOCK_OFFSET_SIZE + BLOCK_ENTRY_COUNT_SIZE);

        self.data.put_bytes(0, offset_section_start - entries_end);

        for offset in &self.offsets {
            self.data.put_u16_le(*offset);
        }

        self.data.put_u16_le(self.len as u16);

        Block {
            data: self.data.freeze(),
            len: self.len,
            offsets: Arc::new(self.offsets),
            first_key: self.first_key,
            last_key: self.last_key,
        }
    }

    pub fn dump(&self) {
        if self.is_empty() {
            println!("<empty_block>");
            return;
        }

        for entry in self.iter() {
            println!(
                "key = {}, revision = {}, position = {}",
                entry.key, entry.revision, entry.position
            );
        }
    }
}

pub struct ScanForward<'a> {
    inner: &'a BlockMut,
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
    inner: &'a BlockMut,
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

pub struct Iter<'a> {
    index: usize,
    max: usize,
    inner: &'a BlockMut,
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
