use crate::index::rannoch::block::BlockEntry;
use md5::digest::typenum::Integer;
use std::cmp::Ordering;
use std::io;

mod rannoch;

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

        if let Some((idx, value)) = lower {
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
