use crate::index::rannoch::block::BlockEntry;
use crate::index::rannoch::in_mem::InMemStorage;
use crate::index::rannoch::mem_table::MemTable;
use crate::index::rannoch::tests::test_ss_table;
use bytes::BytesMut;

#[test]
fn test_mem_table_get() {
    let mut mem_table = MemTable::default();

    mem_table.put(1, 0, 1);
    mem_table.put(2, 0, 2);
    mem_table.put(3, 0, 3);

    assert_eq!(1, mem_table.get(1, 0).unwrap());
    assert_eq!(2, mem_table.get(2, 0).unwrap());
    assert_eq!(3, mem_table.get(3, 0).unwrap());
}

#[test]
fn test_mem_table_iter() {
    let mut mem_table = MemTable::default();

    mem_table.put(1, 0, 0);
    mem_table.put(1, 1, 5);
    mem_table.put(1, 2, 10);

    let mut iter = mem_table.scan(1, ..);
    assert_eq!(
        BlockEntry {
            key: 1,
            revision: 0,
            position: 0,
        },
        iter.next().unwrap()
    );

    assert_eq!(
        BlockEntry {
            key: 1,
            revision: 1,
            position: 5,
        },
        iter.next().unwrap()
    );

    assert_eq!(
        BlockEntry {
            key: 1,
            revision: 2,
            position: 10,
        },
        iter.next().unwrap()
    );

    let mut iter = mem_table.scan(1, 1..);

    assert_eq!(
        BlockEntry {
            key: 1,
            revision: 1,
            position: 5,
        },
        iter.next().unwrap()
    );

    assert_eq!(
        BlockEntry {
            key: 1,
            revision: 2,
            position: 10,
        },
        iter.next().unwrap()
    );
    assert!(iter.next().is_none());

    let mut iter = mem_table.scan(1, 0..=2);

    assert_eq!(
        BlockEntry {
            key: 1,
            revision: 0,
            position: 0,
        },
        iter.next().unwrap()
    );

    assert_eq!(
        BlockEntry {
            key: 1,
            revision: 1,
            position: 5,
        },
        iter.next().unwrap()
    );

    assert_eq!(
        BlockEntry {
            key: 1,
            revision: 2,
            position: 10,
        },
        iter.next().unwrap()
    );

    assert!(iter.next().is_none());
}

#[test]
fn test_mem_table_flush() {
    let mut storage = InMemStorage::new(128);
    let mut table = test_ss_table();
    let mut mem_table = MemTable::default();

    mem_table.put(1, 0, 1);
    mem_table.put(2, 0, 2);
    mem_table.put(3, 0, 3);

    let values = mem_table
        .into_iter()
        .map(|entry| (entry.key, entry.revision, entry.position));

    storage.sst_put(&mut table, values);
    let block = storage.sst_read_block(&table, 0).unwrap();
    block.dump();
    let mut iter = storage.sst_iter(&table);

    let entry = iter.next().unwrap();
    assert_eq!(1, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(1, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(3, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(3, entry.position);

    assert!(iter.next().is_none());
}
