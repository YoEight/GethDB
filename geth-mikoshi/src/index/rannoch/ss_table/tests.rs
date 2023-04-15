use crate::index::rannoch::block::BLOCK_MIN_SIZE;
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
    let mut builder = SsTable::builder(&mut buffer, BLOCK_MIN_SIZE);

    builder.add(1, 2, 3);
    let table = builder.build();
    let entry = table.find_key(1, 2).expect("to be defined");

    assert_eq!(1, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(3, entry.position);
}

#[test]
fn test_sst_build_two_block() {
    let mut buffer = BytesMut::new();
    let mut builder = SsTable::builder(&mut buffer, BLOCK_MIN_SIZE);

    builder.add(1, 2, 3);
    builder.add(2, 3, 4);

    assert_eq!(2, builder.count);

    let table = builder.build();
    let entry = table.find_key(1, 2).expect("to be defined");
    assert_eq!(1, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(3, entry.position);

    let entry = table.find_key(2, 3).expect("to be defined");
    assert_eq!(2, entry.key);
    assert_eq!(3, entry.revision);
    assert_eq!(4, entry.position);
}

#[test]
fn test_sst_key_not_found() {
    let mut buffer = BytesMut::new();
    let mut builder = SsTable::builder(&mut buffer, BLOCK_MIN_SIZE);

    builder.add(1, 2, 3);

    let table = builder.build();

    assert!(table.find_key(1, 3).is_none());
}

#[test]
fn test_sst_encoding() {
    let table = generate_sst();
    let mut buffer = BytesMut::new();
    table.encode(&mut buffer);

    let decoded_table = SsTable::decode(buffer.freeze());

    assert_eq!(table.count, decoded_table.count);

    for i in 0..NUM_OF_KEYS {
        let key = key_of(i);
        let revision = revision_of(i);
        let position = position_of(i);

        let entry = table.find_key(key, revision).expect("to be defined");
        let decoded_entry = decoded_table
            .find_key(key, revision)
            .expect("to be defined");

        assert_eq!(key, entry.key);
        assert_eq!(revision, entry.revision);
        assert_eq!(position, entry.position);
        assert_eq!(entry.key, decoded_entry.key);
        assert_eq!(entry.revision, decoded_entry.revision);
        assert_eq!(entry.position, decoded_entry.position);
    }
}

#[test]
fn test_sst_build_all() {
    generate_sst();
}

#[test]
fn test_sst_find_key() {
    let table = generate_sst();

    for i in 0..NUM_OF_KEYS {
        let key = key_of(i);
        let revision = revision_of(i);
        let position = position_of(i);

        let entry = table.find_key(key, revision).expect("to be defined");

        assert_eq!(key, entry.key);
        assert_eq!(revision, entry.revision);
        assert_eq!(position, entry.position);
    }
}
