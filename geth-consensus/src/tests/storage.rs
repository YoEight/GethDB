use arbitrary::{Arbitrary, Unstructured};
use proptest::proptest;
use rand::Rng;

use crate::entry::{Entry, EntryId};
use crate::tests::arb_entries;
use crate::{IterateEntries, PersistentStorage};

pub struct InMemStorage {
    inner: Vec<Entry>,
}

impl InMemStorage {
    pub fn from(inner: Vec<Entry>) -> Self {
        Self { inner }
    }

    pub fn empty() -> Self {
        Self { inner: Vec::new() }
    }
}

impl PersistentStorage for InMemStorage {
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

impl<'a> Arbitrary<'a> for InMemStorage {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let len = u.arbitrary_len::<Entry>()?;
        let mut inner = Vec::with_capacity(len);

        for _ in 0..len {
            inner.push(u.arbitrary()?);
        }

        inner.sort_by(|a: &Entry, b| (a.index, a.term).cmp(&(b.index, b.term)));

        Ok(Self { inner })
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
        let expected = entries.clone();
        let mut storage = InMemStorage::empty();
        storage.append_entries(entries);

        for (a, b) in expected
            .into_iter()
            .zip(storage.read_entries(0, usize::MAX)
            .collect()
            .unwrap()) {

            assert_eq!(a.index, b.index);
            assert_eq!(a.term, b.term);
        }
    }
}

proptest! {
    #[test]
    fn test_in_mem_contains_entry_non_existing(
        entries in arb_entries(0u64 ..= 100),
        index in 101u64 ..= 200,
        term in 200u64 ..= 300,
    ) {
        let storage = InMemStorage::from(entries);

        assert!(!storage.contains_entry(&EntryId::new(index, term)));
    }
}

proptest! {
    #[test]
    fn test_in_mem_previous_entry_non_empty(
        entries in arb_entries(1u64 ..= 100),
    ) {
        let storage = InMemStorage::from(entries.clone());

        let mut prev_entry: Option<Entry> = None;
        for entry in entries {
            if let Some(actual) = storage.previous_entry(entry.index) {
                println!("Current entry: index {}, term {}", entry.index, entry.term);
                let expected = prev_entry.take().unwrap();

                assert_eq!(expected.index, actual.index);
                assert_eq!(expected.term, actual.term);
            } else {
                println!("(Empty) Current entry: index {}, term {}", entry.index, entry.term);
                assert!(prev_entry.is_none());
            }

            prev_entry = Some(entry);
        }
    }
}

proptest! {
    #[test]
    fn test_in_mem_remove_entries(
        entries in arb_entries(0u64 ..= 100),
    ) {
        // A check to prevent empty-ranges.
        let point = if entries.len() > 1 {
            rand::thread_rng().gen_range(0usize .. entries.len())
        } else {
            0
        };

        let mut storage = InMemStorage::from(entries.clone());
        let entry = if let Some(e) = entries.get(point) {
            e.id()
        } else {
            // Means we are dealing with an empty entry collection.
            EntryId::new(0, 0)
        };

        let mut expected = entries.clone();

        expected.retain(|e| e.index < entry.index);
        storage.remove_entries(&entry);

        let actual = storage.read_all().collect().unwrap();

        assert_eq!(expected.len(), actual.len());

        for (a, b) in expected.iter().zip(actual.iter()) {
            assert_eq!(a.id(), b.id());
        }
    }
}

#[test]
fn test_in_mem_contains_entry_when_empty() {
    let storage = InMemStorage::empty();

    assert!(storage.contains_entry(&EntryId::new(0, 0)));
}
