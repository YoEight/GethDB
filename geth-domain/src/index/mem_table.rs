use std::cmp::Ordering;
use std::collections::BTreeMap;

use bytes::{Buf, BufMut, BytesMut};

use geth_common::{Direction, Revision};

use crate::index::block::BlockEntry;

pub const MEM_TABLE_ENTRY_SIZE: usize = 16;

#[derive(Debug, Clone, Default)]
pub struct MemTable {
    inner: BTreeMap<u64, BytesMut>,
    entries_count: usize,
}

impl MemTable {
    pub fn get(&self, key: u64, revision: u64) -> Option<u64> {
        if let Some(stream) = self.inner.get(&key) {
            let mut bytes = stream.clone();

            while bytes.has_remaining() {
                match revision.cmp(&bytes.get_u64_le()) {
                    Ordering::Less => bytes.advance(8),
                    Ordering::Equal => return Some(bytes.get_u64_le()),
                    Ordering::Greater => return None,
                }
            }
        }

        None
    }

    pub fn put(&mut self, key: u64, revision: u64, position: u64) {
        let stream = self.inner.entry(key).or_default();

        stream.put_u64_le(revision);
        stream.put_u64_le(position);
        self.entries_count += 1;
    }

    pub fn scan(&self, key: u64, direction: Direction, start: Revision<u64>, count: usize) -> Scan {
        let buffer = self.inner.get(&key).cloned().unwrap_or_default();

        Scan::new(key, buffer, direction, start, count)
    }

    pub fn size(&self) -> usize {
        self.entries_count * MEM_TABLE_ENTRY_SIZE
    }

    pub fn entries(self) -> impl Iterator<Item = (u64, u64, u64)> {
        self.into_iter()
            .map(|entry| (entry.key, entry.revision, entry.position))
    }
}

impl IntoIterator for MemTable {
    type Item = BlockEntry;
    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            table: self,
            current: None,
        }
    }
}

fn binary_search_index(buffer: &[u8], revision: u64) -> usize {
    let mut low = 0i64;
    let mut high = (buffer.len() / MEM_TABLE_ENTRY_SIZE) as i64 - 1;

    while low <= high {
        let mid = ((low + high) / 2) as usize;
        let offset = mid * MEM_TABLE_ENTRY_SIZE;
        let mut temp = &buffer[offset..offset + 8];
        let value = temp.get_u64_le();

        match revision.cmp(&value) {
            Ordering::Less => high = mid as i64 - 1,
            Ordering::Equal => return mid,
            Ordering::Greater => low = mid as i64 + 1,
        }
    }

    low as usize
}

pub struct Scan {
    key: u64,
    len: usize,
    index: usize,
    buffer: BytesMut,
    count: usize,
    direction: Direction,
}

impl Scan {
    fn new(
        key: u64,
        buffer: BytesMut,
        direction: Direction,
        start: Revision<u64>,
        mut count: usize,
    ) -> Self {
        let len = buffer.len() / MEM_TABLE_ENTRY_SIZE;
        let mut index = 0usize;

        if len == 0 {
            count = 0;
        } else {
            index = binary_search_index(buffer.as_ref(), start.raw());

            if index >= len - 1 {
                index = len - 1;
            }
        }

        Self {
            key,
            len,
            index,
            buffer,
            count,
            direction,
        }
    }
}

impl Iterator for Scan {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count == 0 {
            return None;
        }

        let offset = self.index * MEM_TABLE_ENTRY_SIZE;
        let mut slice = &self.buffer.as_ref()[offset..offset + MEM_TABLE_ENTRY_SIZE];
        let entry = BlockEntry {
            key: self.key,
            revision: slice.get_u64_le(),
            position: slice.get_u64_le(),
        };

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
                } else {
                    self.count = 0;
                }
            }
        }

        Some(entry)
    }
}

pub struct IntoIter {
    table: MemTable,
    current: Option<Scan>,
}

impl Iterator for IntoIter {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.is_none() {
                let (key, buffer) = self.table.inner.pop_first()?;

                self.current = Some(Scan::new(
                    key,
                    buffer,
                    Direction::Forward,
                    Revision::Start,
                    usize::MAX,
                ));
            }

            let scan = self.current.as_mut()?;
            if let Some(entry) = scan.next() {
                return Some(entry);
            }

            self.current = None;
        }
    }
}
