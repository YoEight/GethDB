#[cfg(test)]
mod tests;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};

pub const MEM_TABLE_ENTRY_SIZE: usize = 8;

#[derive(Debug)]
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

            if revision as usize >= bytes.len() / MEM_TABLE_ENTRY_SIZE {
                return None;
            }

            bytes.advance(revision as usize * MEM_TABLE_ENTRY_SIZE);
            return Some(bytes.get_u64_le());
        }

        None
    }

    pub fn put(&mut self, key: u64, position: u64) {
        if let Some(stream) = self.inner.get_mut(&key) {
            stream.put_u64_le(position);
        } else {
            let mut stream = BytesMut::new();

            stream.put_u64_le(position);
            self.inner.insert(key, stream);
        }
    }

    pub fn scan<R>(&self, key: u64, range: R) -> Scan<R>
    where
        R: RangeBounds<u64>,
    {
        let buffer = self.inner.get(&key).map(|s| s.clone()).unwrap_or_default();

        Scan::new(buffer, range)
    }
}

pub struct Scan<R> {
    current: u64,
    buffer: BytesMut,
    range: R,
}

impl<R> Scan<R>
where
    R: RangeBounds<u64>,
{
    fn new(mut buffer: BytesMut, range: R) -> Self {
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

        Self {
            current,
            buffer,
            range,
        }
    }
}

impl<R> Iterator for Scan<R>
where
    R: RangeBounds<u64>,
{
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.buffer.has_remaining() {
            return None;
        }

        if !self.range.contains(&self.current) {
            self.buffer.advance(self.buffer.remaining());
            return None;
        }

        self.current += 1;

        Some(self.buffer.get_u64_le())
    }
}
