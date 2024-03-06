use rand::Rng;

use crate::entry::{Entry, EntryId};
use crate::IterateEntries;
use crate::PersistentStorage;

mod in_mem;

fn prop_append_entries_and_read_all<S: PersistentStorage>(entries: Vec<Entry>) {
    let expected = entries.clone();

    let mut storage = S::empty();
    storage.append_entries(entries);

    for (a, b) in expected
        .into_iter()
        .zip(storage.read_entries(0, usize::MAX).collect().unwrap())
    {
        assert_eq!(a.index, b.index);
        assert_eq!(a.term, b.term);
    }
}

/// We assume that both `random_index` and `random_term` are not in the submitted entry set.
fn prop_contains_non_existing<S: PersistentStorage>(
    entries: Vec<Entry>,
    random_index: u64,
    random_term: u64,
) {
    let mut storage = S::empty();
    storage.append_entries(entries);

    assert!(!storage.contains_entry(&EntryId::new(random_index, random_term)));
}

fn prop_previous_entry<S: PersistentStorage>(entries: Vec<Entry>) {
    let mut storage = S::empty();
    storage.append_entries(entries.clone());

    let mut prev_entry: Option<Entry> = None;
    for entry in entries {
        if let Some(actual) = storage.previous_entry(entry.index) {
            let expected = prev_entry.take().unwrap();

            assert_eq!(expected.index, actual.index);
            assert_eq!(expected.term, actual.term);
        } else {
            assert!(prev_entry.is_none());
        }

        prev_entry = Some(entry);
    }
}

fn prop_remove_entries<S: PersistentStorage>(entries: Vec<Entry>) {
    let mut storage = S::empty();
    storage.append_entries(entries.clone());

    // A check to prevent empty-ranges.
    let point = if entries.len() > 1 {
        rand::thread_rng().gen_range(0usize..entries.len())
    } else {
        0
    };

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

fn prop_contains_entry_when_empty<S: PersistentStorage>() {
    let storage = S::empty();

    assert!(storage.contains_entry(&EntryId::new(0, 0)));
}
