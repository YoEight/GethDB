use std::io;

use geth_common::IteratorIO;
use geth_mikoshi::InMemoryStorage;

use crate::index::block::BlockEntry;
use crate::index::mem_table::MemTable;
use crate::index::ss_table::SsTable;

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
fn test_mem_table_forward_iter() {
    let mut mem_table = MemTable::default();

    mem_table.put(1, 0, 0);
    mem_table.put(1, 1, 5);
    mem_table.put(1, 2, 10);

    let mut iter = mem_table.scan_forward(1, 0, usize::MAX);
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

    let mut iter = mem_table.scan_forward(1, 1, usize::MAX);

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

    let mut iter = mem_table.scan_forward(1, 0, 3);

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
fn test_mem_table_backward_iter() {
    let mut mem_table = MemTable::default();

    mem_table.put(1, 0, 0);
    mem_table.put(1, 1, 5);
    mem_table.put(1, 2, 10);

    let mut iter = mem_table.scan_backward(1, u64::MAX, usize::MAX);
    assert_eq!(
        BlockEntry {
            key: 1,
            revision: 2,
            position: 10,
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
            revision: 0,
            position: 0,
        },
        iter.next().unwrap()
    );

    assert!(iter.next().is_none());

    let mut iter = mem_table.scan_backward(1, 1, usize::MAX);

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
            revision: 0,
            position: 0,
        },
        iter.next().unwrap()
    );
    assert!(iter.next().is_none());

    let mut iter = mem_table.scan_backward(1, 2, 3);

    assert_eq!(
        BlockEntry {
            key: 1,
            revision: 2,
            position: 10,
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
            revision: 0,
            position: 0,
        },
        iter.next().unwrap()
    );

    assert!(iter.next().is_none());
}

#[test]
fn test_mem_table_flush() -> io::Result<()> {
    let storage = InMemoryStorage::new();
    let mut table = SsTable::new(storage, 128);
    let mut mem_table = MemTable::default();

    mem_table.put(1, 0, 1);
    mem_table.put(2, 0, 2);
    mem_table.put(3, 0, 3);

    let values = mem_table
        .into_iter()
        .map(|entry| (entry.key, entry.revision, entry.position));

    table.put_iter(values)?;
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
