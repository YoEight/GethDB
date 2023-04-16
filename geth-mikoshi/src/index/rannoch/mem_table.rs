#[cfg(test)]
mod tests;

use crate::index::rannoch::block::BlockEntry;
use crate::index::rannoch::ss_table::SsTable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds, RangeFull};

pub const MEM_TABLE_ENTRY_SIZE: usize = 16;

#[derive(Debug, Clone)]
pub struct MemTable {
    inner: BTreeMap<u64, BytesMut>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

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
        if let Some(stream) = self.inner.get_mut(&key) {
            stream.put_u64_le(revision);
            stream.put_u64_le(position);
        } else {
            let mut stream = BytesMut::new();

            stream.put_u64_le(0);
            stream.put_u64_le(position);
            self.inner.insert(key, stream);
        }
    }

    pub fn scan<R>(&self, key: u64, range: R) -> Scan<R>
    where
        R: RangeBounds<u64>,
    {
        let buffer = self.inner.get(&key).map(|s| s.clone()).unwrap_or_default();

        Scan::new(key, buffer, range)
    }

    pub fn flush(self, buffer: &mut BytesMut, block_size: usize) -> SsTable {
        let mut builder = SsTable::builder(buffer, block_size);

        for (key, streams) in self.inner {
            let scan = Scan::new(key, streams, ..);

            for entry in scan {
                builder.add(key, entry.revision, entry.position);
            }
        }

        builder.build()
    }
}

impl IntoIterator for MemTable {
    type Item = BlockEntry;
    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            inner: self.inner.into_iter(),
            current: None,
        }
    }
}

pub struct Scan<R> {
    key: u64,
    buffer: BytesMut,
    range: R,
}

impl<R> Scan<R>
where
    R: RangeBounds<u64>,
{
    fn new(key: u64, mut buffer: BytesMut, range: R) -> Self {
        let current = match range.start_bound() {
            Bound::Included(x) => *x,
            Bound::Excluded(x) => *x + 1,
            Bound::Unbounded => 0,
        };

        let offset = current as usize * MEM_TABLE_ENTRY_SIZE;

        if offset > buffer.remaining() {
            buffer.advance(buffer.remaining());
        } else {
            buffer.advance(offset);
        }

        Self { key, buffer, range }
    }
}

impl<R> Iterator for Scan<R>
where
    R: RangeBounds<u64>,
{
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.buffer.has_remaining() {
            return None;
        }

        let revision = self.buffer.get_u64_le();

        if !self.range.contains(&revision) {
            self.buffer.advance(self.buffer.remaining());
            return None;
        }

        Some(BlockEntry {
            key: self.key,
            revision,
            position: self.buffer.get_u64_le(),
        })
    }
}

pub struct IntoIter {
    inner: std::collections::btree_map::IntoIter<u64, BytesMut>,
    current: Option<Scan<RangeFull>>,
}

impl Iterator for IntoIter {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.is_none() {
                let (key, buffer) = self.inner.next()?;
                self.current = Some(Scan::new(key, buffer, ..));
            }

            let mut scan = self.current.as_mut()?;
            if let Some(entry) = scan.next() {
                return Some(entry);
            }

            self.current = None;
        }
    }
}
