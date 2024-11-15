use std::collections::BTreeMap;
use std::iter::Rev;

use crate::index::block::BlockEntry;

pub const MEM_TABLE_ENTRY_SIZE: usize = 16;

pub enum NoMemTable {}

impl Iterator for NoMemTable {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        unreachable!()
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemTable {
    inner: BTreeMap<u64, BTreeMap<u64, u64>>,
    entries_count: usize,
}

impl MemTable {
    pub fn get(&self, key: u64, revision: u64) -> Option<u64> {
        let stream = self.inner.get(&key)?;
        stream.get(&revision).cloned()
    }

    pub fn put(&mut self, key: u64, revision: u64, position: u64) {
        let stream = self.inner.entry(key).or_default();
        *stream.entry(revision).or_default() = position;

        self.entries_count += 1;
    }

    pub fn scan_forward(&self, key: u64, start: u64, max: usize) -> ScanForward {
        ScanForward {
            key,
            start,
            inner: self.inner.get(&key).map(|x| x.iter()),
            count: 0,
            max,
        }
    }

    pub fn scan_backward(&self, key: u64, start: u64, max: usize) -> ScanBackward {
        ScanBackward {
            key,
            start,
            inner: self.inner.get(&key).map(|x| x.iter().rev()),
            count: 0,
            max,
        }
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
            key: 0,
            table: self,
            current: None,
        }
    }
}

pub struct ScanForward<'a> {
    key: u64,
    start: u64,
    inner: Option<std::collections::btree_map::Iter<'a, u64, u64>>,
    count: usize,
    max: usize,
}

impl<'a> Iterator for ScanForward<'a> {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count >= self.max {
            return None;
        }

        let inner = self.inner.as_mut()?;
        while let Some((rev, pos)) = inner.next() {
            if *rev < self.start {
                continue;
            }

            self.count += 1;

            return Some(BlockEntry {
                key: self.key,
                revision: *rev,
                position: *pos,
            });
        }

        self.count = self.max;
        None
    }
}

pub struct ScanBackward<'a> {
    key: u64,
    start: u64,
    inner: Option<Rev<std::collections::btree_map::Iter<'a, u64, u64>>>,
    count: usize,
    max: usize,
}

impl<'a> Iterator for ScanBackward<'a> {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count >= self.max {
            return None;
        }

        let inner = self.inner.as_mut()?;
        while let Some((rev, pos)) = inner.next() {
            if *rev > self.start {
                continue;
            }

            self.count += 1;

            return Some(BlockEntry {
                key: self.key,
                revision: *rev,
                position: *pos,
            });
        }

        self.count = self.max;
        None
    }
}

pub struct IntoIter {
    table: MemTable,
    key: u64,
    current: Option<std::collections::btree_map::IntoIter<u64, u64>>,
}

impl Iterator for IntoIter {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.is_none() {
                let (key, buffer) = self.table.inner.pop_first()?;

                self.current = Some(buffer.into_iter());
                self.key = key;
            }

            let current = self.current.as_mut()?;
            if let Some((revision, position)) = current.next() {
                return Some(BlockEntry {
                    key: self.key,
                    revision,
                    position,
                });
            }

            self.current = None;
        }
    }
}
