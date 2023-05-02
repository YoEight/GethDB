use crate::index::rannoch::block::BlockEntry;
use crate::index::rannoch::mem_table::MemTable;
use crate::index::rannoch::ss_table::SsTable;
use crate::index::IteratorIO;
use crate::storage::in_mem::InMemoryStorage;
use bytes::BytesMut;
use std::io;

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
fn test_mem_table_flush() -> io::Result<()> {
    let mut storage = InMemoryStorage::new();
    let mut table = SsTable::new(storage, 128);
    let mut buffer = BytesMut::new();
    let mut mem_table = MemTable::default();

    mem_table.put(1, 0, 1);
    mem_table.put(2, 0, 2);
    mem_table.put(3, 0, 3);

    let values = mem_table
        .into_iter()
        .map(|entry| (entry.key, entry.revision, entry.position));

    table.put_iter(&mut buffer, values)?;
    let block = table.read_block(0)?;
    block.dump();
    let mut iter = table.iter();

    let entry = iter.next()?.unwrap();
    assert_eq!(1, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(1, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(3, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(3, entry.position);

    assert!(iter.next()?.is_none());

    Ok(())
}
