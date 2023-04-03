use std::collections::{BTreeSet, HashMap};

pub struct EventIndex {
    pub revision: u64,
    pub position: u64,
}

#[derive(Default)]
pub struct MemTable {
    inner: HashMap<u64, BTreeSet<EventIndex>>,
}

impl MemTable {
    pub fn range(&self, stream_id_hash: u64, start: u64, end: u64) -> Option<RangeIndex> {
        if let Some(set) = self.inner.get(&stream_id_hash) {
            Some(RangeIndex::new(start, end, set));
        }

        None
    }
}

pub struct RangeIndex<'a> {
    start: u64,
    end: u64,
    inner: std::collections::btree_set::Iter<'a, EventIndex>,
    complete: bool,
}

impl<'a> RangeIndex<'a> {
    fn new(start: u64, end: u64, base: &'a BTreeSet<EventIndex>) -> Self {
        Self {
            start,
            end,
            inner: base.iter(),
            complete: false,
        }
    }
}

impl<'a> Iterator for RangeIndex<'a> {
    type Item = &'a EventIndex;

    fn next(&mut self) -> Option<Self::Item> {
        if self.complete {
            return None;
        }

        while let Some(entry) = self.inner.next() {
            if entry.revision < self.start {
                continue;
            }

            if entry.revision > self.end {
                break;
            }

            return Some(entry);
        }

        self.complete = true;
        None
    }
}
