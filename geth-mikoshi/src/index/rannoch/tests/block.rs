use crate::index::rannoch::block::{Block, BLOCK_ENTRY_SIZE};
use crate::index::rannoch::in_mem::InMemStorage;
use crate::index::rannoch::ss_table::SsTable;
use crate::index::rannoch::tests::{key_of, position_of, revision_of, test_ss_table, NUM_OF_KEYS};
use bytes::BytesMut;

fn in_mem_generate_block(storage: &mut InMemStorage, table: &mut SsTable) {
    let values = (0..NUM_OF_KEYS).map(|idx| (key_of(idx), revision_of(idx), position_of(idx)));
    storage.sst_put(table, values);
}

#[test]
fn test_in_mem_block_build_single_key() {
    let mut storage = InMemStorage::new(BLOCK_ENTRY_SIZE);
    let mut table = SsTable::new();

    storage.sst_put(&mut table, [(1, 1, 1)]);

    assert_eq!(1, table.len());

    let block = storage.sst_read_block(&table, 0).unwrap();
    block.dump();
    let entry = block.read_entry(0).unwrap();

    assert_eq!(1, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(1, entry.position);
}

#[test]
fn test_in_mem_block_build_full() {
    let mut storage = InMemStorage::new(BLOCK_ENTRY_SIZE);
    let mut table = SsTable::new();

    storage.sst_put(&mut table, [(1, 1, 10), (2, 2, 20)]);

    assert_eq!(2, table.len());

    for idx in 0..table.len() {
        let entry = storage
            .sst_read_block(&table, idx)
            .unwrap()
            .read_entry(0)
            .unwrap();

        assert_eq!((idx + 1) as u64, entry.key);
        assert_eq!((idx + 1) as u64, entry.revision);
        assert_eq!(((idx + 1) * 10) as u64, entry.position);
    }
}

#[test]
fn test_in_mem_block_build_all() {
    let mut storage = InMemStorage::default();
    let mut table = SsTable::new();

    in_mem_generate_block(&mut storage, &mut table);

    let block = storage.sst_read_block(&table, 0).unwrap();

    for idx in 0..NUM_OF_KEYS {
        let entry = block.read_entry(idx).unwrap();

        assert_eq!(key_of(idx), entry.key);
        assert_eq!(revision_of(idx), entry.revision);
        assert_eq!(position_of(idx), entry.position);
    }
}

#[test]
fn test_in_mem_block_find_key() {
    let mut storage = InMemStorage::default();
    let mut table = test_ss_table();

    in_mem_generate_block(&mut storage, &mut table);

    let block = storage.sst_read_block(&table, 0).unwrap();

    for i in 0..NUM_OF_KEYS {
        let entry = block
            .find_entry(key_of(i), revision_of(i))
            .expect("entry to be defined");

        assert_eq!(
            entry.key,
            key_of(i),
            "expected key: {:?}, actual key: {:?}",
            key_of(i),
            entry.key
        );
        assert_eq!(
            entry.revision,
            revision_of(i),
            "expected revision: {:?}, actual revision: {:?}",
            revision_of(i),
            entry.revision,
        );
        assert_eq!(
            entry.position,
            position_of(i),
            "expected position: {:?}, actual position: {:?}",
            position_of(i),
            entry.position,
        );
    }
}

#[test]
fn test_in_mem_block_scan_skipped() {
    let mut storage = InMemStorage::default();
    let mut table = SsTable::new();

    storage.sst_put(
        &mut table,
        [
            (1, 0, 1),
            (1, 1, 2),
            (1, 2, 3),
            (2, 0, 4),
            (2, 2, 6),
            (3, 0, 7),
            (3, 1, 8),
            (3, 2, 9),
        ],
    );

    let block = storage.sst_read_block(&table, 0).unwrap();
    let mut iter = block.scan(2, ..);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(4, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(6, entry.position);

    assert!(iter.next().is_none());
}

#[test]
fn test_in_mem_block_scan() {
    let mut storage = InMemStorage::default();
    let mut table = SsTable::new();

    storage.sst_put(
        &mut table,
        [
            (1, 0, 1),
            (1, 1, 2),
            (1, 2, 3),
            (2, 0, 4),
            (2, 1, 5),
            (2, 2, 6),
            (3, 0, 7),
            (3, 1, 8),
            (3, 2, 9),
        ],
    );

    let block = storage.sst_read_block(&table, 0).unwrap();
    let mut iter = block.scan(2, ..);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(4, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(6, entry.position);

    assert!(iter.next().is_none());
}

#[test]
fn test_in_mem_block_scan_gap() {
    let mut storage = InMemStorage::default();
    let mut table = SsTable::new();

    storage.sst_put(&mut table, [(2, 1, 5), (2, 2, 6)]);

    let block = storage.sst_read_block(&table, 0).unwrap();
    let mut iter = block.scan(2, ..);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(6, entry.position);

    assert!(iter.next().is_none());
}

#[test]
fn test_in_mem_block_scan_not_found_easy_1() {
    let mut storage = InMemStorage::default();
    let mut table = SsTable::new();

    storage.sst_put(&mut table, [(3, 0, 7), (3, 1, 8), (3, 2, 9)]);

    let block = storage.sst_read_block(&table, 0).unwrap();
    let mut iter = block.scan(2, ..);

    assert!(iter.next().is_none());
}

#[test]
fn test_in_mem_block_scan_not_found_easy_2() {
    let mut storage = InMemStorage::default();
    let mut table = SsTable::new();

    storage.sst_put(&mut table, [(1, 0, 1), (1, 1, 2), (1, 2, 3)]);

    let block = storage.sst_read_block(&table, 0).unwrap();
    let mut iter = block.scan(2, ..);

    assert!(iter.next().is_none());
}

#[test]
fn test_in_mem_block_scan_not_found_hard() {
    let mut storage = InMemStorage::default();
    let mut table = SsTable::new();

    storage.sst_put(
        &mut table,
        [
            (1, 0, 1),
            (1, 1, 2),
            (1, 2, 3),
            (3, 0, 7),
            (3, 1, 8),
            (3, 2, 9),
        ],
    );

    let block = storage.sst_read_block(&table, 0).unwrap();
    let mut iter = block.scan(2, ..);

    assert!(iter.next().is_none());
}
