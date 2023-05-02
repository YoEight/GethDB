use crate::index::rannoch::block::{BLOCK_ENTRY_SIZE, BLOCK_MIN_SIZE};
use crate::index::rannoch::ss_table::SsTable;
use crate::index::rannoch::tests::{
    in_mem_generate_sst, key_of, position_of, revision_of, NUM_OF_KEYS,
};
use crate::index::IteratorIO;
use crate::storage::in_mem::InMemoryStorage;
use bytes::BytesMut;
use std::io;
use uuid::Uuid;

#[test]
fn test_in_mem_sst_build_single_key() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::new(InMemoryStorage::new(), BLOCK_ENTRY_SIZE);

    table.put_iter(&mut buffer, [(1, 2, 3)])?;

    let entry = table.find_key(1, 2)?.unwrap();

    assert_eq!(1, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(3, entry.position);

    Ok(())
}

#[test]
fn test_in_mem_sst_build_two_blocks() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::new(InMemoryStorage::new(), BLOCK_ENTRY_SIZE);

    table.put_iter(&mut buffer, [(1, 2, 3), (2, 3, 4)])?;

    let entry = table.find_key(1, 2)?.unwrap();

    assert_eq!(1, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(3, entry.position);

    let entry = table.find_key(2, 3)?.unwrap();

    assert_eq!(2, entry.key);
    assert_eq!(3, entry.revision);
    assert_eq!(4, entry.position);

    Ok(())
}

#[test]
fn test_in_mem_sst_key_not_found() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::new(InMemoryStorage::new(), BLOCK_ENTRY_SIZE);

    table.put_iter(&mut buffer, [(1, 2, 3)])?;

    assert!(table.find_key(1, 3)?.is_none());

    Ok(())
}

#[test]
fn test_in_mem_sst_find_key() -> io::Result<()> {
    let table = in_mem_generate_sst();

    for i in 0..NUM_OF_KEYS {
        let key = key_of(i);
        let revision = revision_of(i);
        let position = position_of(i);

        let entry = table.find_key(key, revision)?.expect("to be defined");

        assert_eq!(key, entry.key);
        assert_eq!(revision, entry.revision);
        assert_eq!(position, entry.position);
    }

    Ok(())
}

#[test]
fn test_in_mem_ss_table_scan() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::with_default(InMemoryStorage::new());

    table.put_iter(
        &mut buffer,
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
    )?;

    let mut iter = table.scan(2, ..);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(4, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(6, entry.position);

    assert!(iter.next()?.is_none());

    Ok(())
}

#[test]
fn test_in_mem_ss_table_scan_3_blocks() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::new(InMemoryStorage::new(), BLOCK_ENTRY_SIZE * 3);

    table.put_iter(
        &mut buffer,
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
    )?;

    assert_eq!(3, table.len());

    let mut iter = table.scan(2, ..);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(4, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(6, entry.position);

    assert!(iter.next()?.is_none());

    Ok(())
}
#[test]

fn test_in_mem_ss_table_scan_not_found() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::new(InMemoryStorage::new(), BLOCK_ENTRY_SIZE * 3);

    table.put_iter(
        &mut buffer,
        [
            (1, 0, 1),
            (1, 1, 2),
            (1, 2, 3),
            (3, 0, 7),
            (3, 1, 8),
            (3, 2, 9),
        ],
    )?;

    let mut iter = table.scan(2, ..);

    assert!(iter.next()?.is_none());

    Ok(())
}
