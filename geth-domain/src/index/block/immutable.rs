use bytes::{Buf, Bytes};
use std::{cmp::Ordering, sync::Arc};

use super::{BlockEntry, BLOCK_ENTRY_SIZE, BLOCK_KEY_SIZE, BLOCK_OFFSET_SIZE};

#[derive(Debug, Clone)]
pub struct Block {
    pub(crate) data: Bytes,
    pub(crate) len: usize,
    pub(crate) offsets: Arc<Vec<u16>>,
    pub(crate) first_key: Option<u64>,
    pub(crate) last_key: Option<u64>,
}

impl Block {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn from(capacity: usize, bytes: Bytes) -> Block {
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
            offsets: Arc::new(offsets),
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

    pub fn scan_forward(&self, key: u64, start: u64, max: usize) -> ScanForward {
        if let Some(first) = self.first_key {
            if first > key {
                return ScanForward {
                    inner: self.clone(),
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
                inner: self.clone(),
                key,
                start,
                count: max,
                max,
                index: None,
            };
        }

        ScanForward {
            inner: self.clone(),
            key,
            start,
            count: 0,
            max,
            index: None,
        }
    }

    pub fn scan_backward(&self, key: u64, start: u64, max: usize) -> ScanBackward {
        if let Some(last) = self.last_key {
            if last < key {
                return ScanBackward {
                    inner: self.clone(),
                    key,
                    start,
                    count: max,
                    max,
                    index: None,
                    anchored: false,
                };
            }
        }

        if self.is_empty() {
            return ScanBackward {
                inner: self.clone(),
                key,
                start,
                count: max,
                max,
                index: None,
                anchored: false,
            };
        }

        ScanBackward {
            inner: self.clone(),
            key,
            start,
            count: 0,
            max,
            index: None,
            anchored: false,
        }
    }

    pub fn contains(&self, key: u64) -> bool {
        self.first_key
            .zip(self.last_key)
            .map(|(f, l)| f <= key && key <= l)
            .unwrap_or_default()
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

fn find_closest_entry(
    block: &Block,
    key: u64,
    revision: u64,
) -> Result<(usize, BlockEntry), usize> {
    let mut low = 0i64;
    let mut high = (block.len() - 1) as i64;

    while low <= high {
        let mid = (low + high) / 2;
        let entry = block.try_read(mid as usize).unwrap();

        match entry.cmp_key_rev(key, revision) {
            Ordering::Less => low = mid + 1,
            Ordering::Greater => high = mid - 1,
            Ordering::Equal => {
                return Ok((mid as usize, entry));
            }
        }
    }

    Err(low as usize)
}

pub struct ScanForward {
    inner: Block,
    key: u64,
    start: u64,
    count: usize,
    max: usize,
    index: Option<usize>,
}

impl Iterator for ScanForward {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.count >= self.max {
                return None;
            }

            if self.index.is_none() {
                match find_closest_entry(&self.inner, self.key, self.start) {
                    Err(edge) => {
                        self.index = Some(edge);
                    }

                    Ok((entry_index, entry)) => {
                        self.count += 1;
                        self.index = Some(entry_index + 1);

                        return Some(entry);
                    }
                }
            }

            let current_idx = self.index?;
            if let Some(entry) = self.inner.try_read(self.index?) {
                self.index = Some(current_idx + 1);

                if self.key > entry.key {
                    continue;
                }

                if self.key < entry.key {
                    self.count = self.max;
                    return None;
                }

                if self.start > entry.revision {
                    continue;
                }

                self.count += 1;

                return Some(entry);
            }

            self.count = self.max;
            return None;
        }
    }
}

pub struct ScanBackward {
    inner: Block,
    key: u64,
    start: u64,
    count: usize,
    max: usize,
    index: Option<usize>,
    anchored: bool,
}

impl Iterator for ScanBackward {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.count >= self.max {
                return None;
            }

            if self.index.is_none() {
                match find_closest_entry(&self.inner, self.key, self.start) {
                    Err(edge) => {
                        if edge > 0 {
                            self.index = Some((edge - 1) as usize);
                        } else {
                            self.count = self.max;
                            return None;
                        }
                    }

                    Ok((entry_index, entry)) => {
                        self.count += 1;

                        if entry_index > 0 {
                            self.index = Some((entry_index - 1) as usize);
                        } else {
                            self.count = self.max;
                        }

                        return Some(entry);
                    }
                }
            }

            let current_idx = self.index?;
            if let Some(entry) = self.inner.try_read(self.index?) {
                // when scanning backward, if start is set to u64::MAX, it means we want to start to the highest revision
                // possible.
                if self.key == entry.key
                    && !self.anchored
                    && (self.start == u64::MAX || self.start >= entry.revision)
                {
                    self.anchored = true;
                }

                if self.key > entry.key
                    || (self.key == entry.key && !self.anchored && self.start > entry.revision)
                {
                    if let Some(new_index) = current_idx.checked_sub(1) {
                        self.index = Some(new_index);
                        continue;
                    }

                    self.count = self.max;
                    return None;
                } else if self.key < entry.key {
                    self.count = self.max;
                    return None;
                }

                self.count += 1;

                if let Some(new_index) = current_idx.checked_sub(1) {
                    self.index = Some(new_index);
                } else {
                    self.count = self.max;
                }

                return Some(entry);
            }

            self.count = self.max;
            return None;
        }
    }
}
