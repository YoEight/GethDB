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
fn test_sst_build_single_key() {
    let mut buffer = BytesMut::new();
    let mut builder = SsTable::builder(&mut buffer, 26);

    builder.add(1, 2, 3);
    let table = builder.build();
    let entry = table.find_key(1, 2).expect("to be defined");

    assert_eq!(1, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(3, entry.position);
}

#[test]
fn test_sst_build_all() {
    generate_sst();
}
