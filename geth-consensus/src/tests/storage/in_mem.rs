use proptest::proptest;

use crate::entry::{Entry, EntryId};
use crate::tests::arb_entries;
use crate::tests::storage::{
    prop_append_entries_and_read_all, prop_contains_entry_when_empty, prop_contains_non_existing,
    prop_previous_entry, prop_remove_entries,
};
use crate::{IterateEntries, PersistentStorage};

pub struct InMemStorage {
    inner: Vec<Entry>,
}

impl PersistentStorage for InMemStorage {
    fn empty() -> Self {
        Self { inner: Vec::new() }
    }

    fn append_entries(&mut self, entries: Vec<Entry>) {
        self.inner.extend(entries);
    }

    fn read_entries(&self, index: u64, max_count: usize) -> impl IterateEntries {
        InMemIter {
            inner: &self.inner,
            start: index,
            limit: max_count,
            count: 0,
            offset: 0,
            init: false,
        }
    }

    fn remove_entries(&mut self, from: &EntryId) {
        self.inner.retain(|e| e.index < from.index);
    }

    fn last_entry(&self) -> Option<EntryId> {
        self.inner.last().map(|e| EntryId {
            index: e.index,
            term: e.term,
        })
    }

    fn previous_entry(&self, index: u64) -> Option<EntryId> {
        let mut prev = None;

        for entry in &self.inner {
            if entry.index >= index {
                break;
            }

            prev = Some(EntryId {
                index: entry.index,
                term: entry.term,
            });
        }

        prev
    }

    fn contains_entry(&self, entry_id: &EntryId) -> bool {
        if self.inner.is_empty() && entry_id.index == 0 {
            return true;
        }

        self.inner
            .iter()
            .find(|e| e.index == entry_id.index && e.term == entry_id.term)
            .is_some()
    }
}

struct InMemIter<'a> {
    inner: &'a Vec<Entry>,
    start: u64,
    limit: usize,
    count: usize,
    offset: usize,
    init: bool,
}

impl<'a> IterateEntries for InMemIter<'a> {
    fn next(&mut self) -> std::io::Result<Option<Entry>> {
        if self.count >= self.limit {
            return Ok(None);
        }

        if !self.init {
            for (pos, entry) in self.inner.iter().enumerate() {
                self.offset = pos;

                if entry.index >= self.start {
                    break;
                }
            }

            self.init = true;
        }

        if let Some(entry) = self.inner.get(self.offset) {
            self.offset += 1;
            return Ok(Some(entry.clone()));
        }

        Ok(None)
    }
}

proptest! {
    #[test]
    fn test_in_mem_append_entries_and_read_all(
        entries in arb_entries(0u64 ..= 100),
    ) {
        prop_append_entries_and_read_all::<InMemStorage>(entries);
    }
}

proptest! {
    #[test]
    fn test_in_mem_contains_entry_non_existing(
        entries in arb_entries(0u64 ..= 100),
        index in 101u64 ..= 200,
        term in 200u64 ..= 300,
    ) {
        prop_contains_non_existing::<InMemStorage>(entries, index, term);
    }
}

proptest! {
    #[test]
    fn test_in_mem_previous_entry_non_empty(
        entries in arb_entries(1u64 ..= 100),
    ) {
        prop_previous_entry::<InMemStorage>(entries);
    }
}

proptest! {
    #[test]
    fn test_in_mem_remove_entries(
        entries in arb_entries(0u64 ..= 100),
    ) {
       prop_remove_entries::<InMemStorage>(entries);
    }
}

#[test]
fn test_in_mem_contains_entry_when_empty() {
    prop_contains_entry_when_empty::<InMemStorage>();
}
