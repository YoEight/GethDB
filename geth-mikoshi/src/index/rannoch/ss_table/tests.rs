use crate::index::rannoch::ss_table::SsTable;
use bytes::BytesMut;

const NUM_OF_KEYS: usize = 100;

fn key_of(idx: usize) -> u64 {
    idx as u64 * 5
}

fn revision_of(idx: usize) -> u64 {
    idx as u64 * 42
}

fn position_of(idx: usize) -> u64 {
    idx as u64
}

fn generate_sst() -> SsTable {
    let mut buffer = BytesMut::new();
    let mut builder = SsTable::builder(&mut buffer, 128);

    for idx in 0..NUM_OF_KEYS {
        builder.add(key_of(idx), revision_of(idx), position_of(idx));
    }

    builder.build()
}

#[test]
fn test_sst_build_all() {
    generate_sst();
}
