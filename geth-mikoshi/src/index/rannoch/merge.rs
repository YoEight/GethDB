#[cfg(test)]
mod tests;

use crate::index::rannoch::block::BlockEntry;
use std::cmp::Ordering;

pub struct Merge<I> {
    pub iters: Vec<I>,
    pub caches: Vec<Option<BlockEntry>>,
}

impl<I> Merge<I>
where
    I: Iterator<Item = BlockEntry>,
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

    fn fill_caches(&mut self) -> Option<()> {
        let mut found = false;

        for (idx, cell) in self.caches.iter_mut().enumerate() {
            if cell.is_none() {
                let value = self.iters[idx].next();
                found |= value.is_some();
                *cell = value;
            } else {
                found = true;
            }
        }

        if found {
            return Some(());
        }

        None
    }
}

impl<I> Iterator for Merge<I>
where
    I: Iterator<Item = BlockEntry>,
{
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.fill_caches()?;
        self.pull_from_cache()
    }
}
