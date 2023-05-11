pub(crate) mod block;
pub(crate) mod lsm;
mod mem_table;
mod merge;
mod ss_table;
#[cfg(test)]
mod tests;

use crate::index::block::BlockEntry;
use std::cmp::Ordering;
use std::collections::Bound;
use std::io;
use std::ops::RangeBounds;

pub use lsm::{Lsm, LsmSettings};

pub trait IteratorIO {
    type Item;

    fn next(&mut self) -> io::Result<Option<Self::Item>>;

    fn map<F, A>(self, func: F) -> Map<F, Self>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> A,
    {
        Map { func, inner: self }
    }

    fn last(mut self) -> io::Result<Option<Self::Item>>
    where
        Self: Sized,
    {
        let mut result = None;

        while let Some(value) = self.next()? {
            result = Some(value);
        }

        Ok(result)
    }
}

pub struct Map<F, I> {
    func: F,
    inner: I,
}

impl<F, A, I> IteratorIO for Map<F, I>
where
    I: IteratorIO,
    F: FnMut(I::Item) -> A,
{
    type Item = A;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        if let Some(item) = self.inner.next()? {
            return Ok(Some((self.func)(item)));
        }

        Ok(None)
    }
}

pub struct MergeIO<I> {
    pub iters: Vec<I>,
    pub caches: Vec<Option<BlockEntry>>,
}

impl<I> MergeIO<I>
where
    I: IteratorIO<Item = BlockEntry>,
{
    pub fn new(iters: Vec<I>) -> Self {
        let mut caches = Vec::with_capacity(iters.len());

        for _ in 0..iters.len() {
            caches.push(None);
        }

        Self { iters, caches }
    }

    fn pull_from_cache(&mut self) -> Option<BlockEntry> {
        let mut lower: Option<(usize, BlockEntry)> = None;

        for (idx, cell) in self.caches.iter_mut().enumerate() {
            if let Some(cell_value) = *cell {
                if let Some((entry_idx, entry)) = lower.as_mut() {
                    match entry.cmp_key_id(&cell_value) {
                        Ordering::Less => continue,
                        Ordering::Equal => *cell = None,
                        Ordering::Greater => {
                            *entry_idx = idx;
                            *entry = cell_value;
                        }
                    }
                } else {
                    lower = Some((idx, cell_value));
                }
            }
        }

        if let Some((idx, _value)) = lower {
            return self.caches[idx].take();
        }

        None
    }

    fn fill_caches(&mut self) -> io::Result<bool> {
        let mut found = false;

        for (idx, cell) in self.caches.iter_mut().enumerate() {
            if cell.is_none() {
                let value = self.iters[idx].next()?;
                found |= value.is_some();
                *cell = value;
            } else {
                found = true;
            }
        }

        Ok(found)
    }
}

impl<I> IteratorIO for MergeIO<I>
where
    I: IteratorIO<Item = BlockEntry>,
{
    type Item = BlockEntry;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        if self.fill_caches()? {
            return Ok(self.pull_from_cache());
        }

        Ok(None)
    }
}

pub struct Lift<I> {
    inner: I,
}

impl<I> IteratorIO for Lift<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        Ok(self.inner.next())
    }
}

impl<I> IteratorIO for Box<I>
where
    I: IteratorIO + ?Sized,
{
    type Item = I::Item;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        self.as_mut().next()
    }
}

pub trait IteratorIOExt: Sized {
    fn lift(self) -> Lift<Self>;
}

impl<I> IteratorIOExt for I
where
    I: Sized,
    I: Iterator,
{
    fn lift(self) -> Lift<Self> {
        Lift { inner: self }
    }
}

#[derive(Copy, Clone)]
pub struct IndexedPosition {
    pub revision: u64,
    pub position: u64,
}

#[derive(Clone)]
pub struct Rev<R> {
    pub inner: R,
}

impl<R> Rev<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R> RangeBounds<u64> for Rev<R>
where
    R: RangeBounds<u64>,
{
    fn start_bound(&self) -> Bound<&u64> {
        self.inner.end_bound()
    }

    fn end_bound(&self) -> Bound<&u64> {
        self.inner.start_bound()
    }
}

#[derive(Debug, Copy, Clone)]
pub enum Range {
    Inbound,
    Outbound(Ordering),
}

pub fn range_start<R>(range: &R) -> u64
where
    R: RangeBounds<u64>,
{
    match range.start_bound() {
        Bound::Included(start) => *start,
        Bound::Excluded(start) => *start + 1,
        Bound::Unbounded => 0,
    }
}

pub fn range_start_decr<R>(range: &R) -> u64
where
    R: RangeBounds<u64>,
{
    match range.end_bound() {
        Bound::Included(start) => *start,
        Bound::Excluded(start) => *start - 1,
        Bound::Unbounded => u64::MAX,
    }
}

pub fn range_end<R>(range: &R) -> u64
where
    R: RangeBounds<u64>,
{
    match range.start_bound() {
        Bound::Included(start) => *start,
        Bound::Excluded(start) => *start + 1,
        Bound::Unbounded => u64::MAX,
    }
}

pub fn in_range<R>(range: &R, value: u64) -> Range
where
    R: RangeBounds<u64>,
{
    match (range.start_bound(), range.end_bound()) {
        (Bound::Included(start), Bound::Included(end)) => {
            if value < *start {
                return Range::Outbound(Ordering::Less);
            }

            if value > *end {
                return Range::Outbound(Ordering::Greater);
            }

            return Range::Inbound;
        }

        (Bound::Included(start), Bound::Excluded(end)) => {
            if value < *start {
                return Range::Outbound(Ordering::Less);
            }

            if value > *end - 1 {
                return Range::Outbound(Ordering::Greater);
            }

            return Range::Inbound;
        }

        (Bound::Included(start), Bound::Unbounded) => {
            if value < *start {
                return Range::Outbound(Ordering::Less);
            }

            return Range::Inbound;
        }

        (Bound::Excluded(start), Bound::Excluded(end)) => {
            if value < *start + 1 {
                return Range::Outbound(Ordering::Less);
            }

            if value > *end - 1 {
                return Range::Outbound(Ordering::Greater);
            }

            return Range::Inbound;
        }

        (Bound::Excluded(start), Bound::Included(end)) => {
            if value < *start + 1 {
                return Range::Outbound(Ordering::Less);
            }

            if value > *end {
                return Range::Outbound(Ordering::Greater);
            }

            return Range::Inbound;
        }

        (Bound::Excluded(start), Bound::Unbounded) => {
            if value < *start + 1 {
                return Range::Outbound(Ordering::Less);
            }

            return Range::Inbound;
        }

        (Bound::Unbounded, Bound::Included(end)) => {
            if value <= *end {
                return Range::Inbound;
            }

            Range::Outbound(Ordering::Greater)
        }

        (Bound::Unbounded, Bound::Excluded(end)) => {
            if value <= *end - 1 {
                return Range::Inbound;
            }

            Range::Outbound(Ordering::Greater)
        }

        (Bound::Unbounded, Bound::Unbounded) => Range::Inbound,
    }
}
