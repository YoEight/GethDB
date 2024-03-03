use bytes::Bytes;
use proptest::collection::vec;
use proptest::prelude::{any, Strategy};
use proptest::prop_compose;

use crate::entry::Entry;

mod storage;

prop_compose! {
    pub fn arb_entry(index_range: impl Strategy<Value = u64>)(
        index in index_range,
        term in 1u64..=100,
        payload in vec(any::<u8>(), 0 ..= 10),
    ) -> Entry {
        Entry {
            index,
            term,
            payload: Bytes::from(payload),
        }
    }
}

prop_compose! {
    pub fn arb_entries(index_range: impl Strategy<Value = u64>)(
        mut entries in vec(arb_entry(index_range), 0 ..= 100),
    ) -> Vec<Entry> {
        entries.sort_by(|a: &Entry, b| (a.index, a.term).cmp(&(b.index, b.term)));
        entries
    }
}
