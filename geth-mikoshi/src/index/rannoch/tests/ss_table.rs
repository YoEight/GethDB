use crate::index::rannoch::block::{BLOCK_ENTRY_SIZE, BLOCK_MIN_SIZE};
use crate::index::rannoch::in_mem::InMemStorage;
use crate::index::rannoch::ss_table::SsTable;
use crate::index::rannoch::tests::{
    in_mem_generate_sst, key_of, position_of, revision_of, test_ss_table, NUM_OF_KEYS,
};
use bytes::BytesMut;
use uuid::Uuid;

#[test]
fn test_in_mem_sst_build_single_key() {
    let mut storage = InMemStorage::new(BLOCK_ENTRY_SIZE);
    let mut table = test_ss_table();

    storage.sst_put(&mut table, [(1, 2, 3)]);

    let entry = storage.sst_find_key(&table, 1, 2).unwrap();

    assert_eq!(1, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(3, entry.position);
}

#[test]
fn test_in_mem_sst_build_two_blocks() {
    let mut storage = InMemStorage::new(BLOCK_ENTRY_SIZE);
    let mut table = test_ss_table();

    storage.sst_put(&mut table, [(1, 2, 3), (2, 3, 4)]);

    let entry = storage.sst_find_key(&table, 1, 2).unwrap();

    assert_eq!(1, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(3, entry.position);

    let entry = storage.sst_find_key(&table, 2, 3).unwrap();

    assert_eq!(2, entry.key);
    assert_eq!(3, entry.revision);
    assert_eq!(4, entry.position);
}

#[test]
fn test_in_mem_sst_key_not_found() {
    let mut storage = InMemStorage::new(BLOCK_ENTRY_SIZE);
    let mut table = test_ss_table();

    storage.sst_put(&mut table, [(1, 2, 3)]);

    assert!(storage.sst_find_key(&table, 1, 3).is_none());
}

#[test]
fn test_in_mem_sst_find_key() {
    let storage = in_mem_generate_sst();
    let table = storage.sst_load(Uuid::nil()).unwrap();

    for i in 0..NUM_OF_KEYS {
        let key = key_of(i);
        let revision = revision_of(i);
        let position = position_of(i);

        let entry = storage
            .sst_find_key(&table, key, revision)
            .expect("to be defined");

        assert_eq!(key, entry.key);
        assert_eq!(revision, entry.revision);
        assert_eq!(position, entry.position);
    }
}
