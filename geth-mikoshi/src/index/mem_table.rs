use crate::index::block::BlockEntry;

use crate::index::{range_start, range_start_decr, Rev};
use bytes::{Buf, BufMut, BytesMut};
use geth_common::Direction;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::{RangeBounds, RangeFull};

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

    pub fn scan_forward<R>(&self, key: u64, range: R) -> Scan<R>
    where
        R: RangeBounds<u64>,
    {
        let buffer = self.inner.get(&key).map(|s| s.clone()).unwrap_or_default();

        Scan::new(key, buffer, Direction::Forward, range)
    }

    pub fn scan_backward<R>(&self, key: u64, range: R) -> Scan<Rev<R>>
    where
        R: RangeBounds<u64>,
    {
        let buffer = self.inner.get(&key).map(|s| s.clone()).unwrap_or_default();

        Scan::new(key, buffer, Direction::Backward, Rev::new(range))
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
            inner: self.inner.into_iter(),
            current: None,
        }
    }
}

pub struct Scan<R> {
    key: u64,
    current: usize,
    end: usize,
    buffer: BytesMut,
    range: R,
    direction: Direction,
}

impl<R> Scan<R>
where
    R: RangeBounds<u64>,
{
    fn new(key: u64, buffer: BytesMut, direction: Direction, range: R) -> Self {
        let count = buffer.len() / MEM_TABLE_ENTRY_SIZE;
        let current = if let Direction::Forward = direction {
            range_start(&range)
        } else {
            let start = range_start_decr(&range);

            if start == u64::MAX {
                (count as u64).checked_sub(1).unwrap_or(u64::MAX)
            } else {
                start
            }
        };

        Self {
            key,
            current: current as usize,
            end: buffer.len() / MEM_TABLE_ENTRY_SIZE,
            buffer,
            range,
            direction,
        }
    }
}

impl<R> Iterator for Scan<R>
where
    R: RangeBounds<u64>,
{
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        match self.direction {
            Direction::Forward if self.current >= self.end => return None,
            Direction::Backward if self.current == usize::MAX => return None,
            _ => {}
        }

        let offset = self.current * MEM_TABLE_ENTRY_SIZE;
        let mut slice = &self.buffer.as_ref()[offset..offset + MEM_TABLE_ENTRY_SIZE];
        let revision = slice.get_u64_le();

        if !self.range.contains(&revision) {
            return None;
        }

        let position = slice.get_u64_le();

        match self.direction {
            Direction::Forward => self.current += 1,
            Direction::Backward => self.current = self.current.checked_sub(1).unwrap_or(usize::MAX),
        }

        Some(BlockEntry {
            key: self.key,
            revision,
            position,
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
                self.current = Some(Scan::new(key, buffer, Direction::Forward, ..));
            }

            let scan = self.current.as_mut()?;
            if let Some(entry) = scan.next() {
                return Some(entry);
            }

            self.current = None;
        }
    }
}
