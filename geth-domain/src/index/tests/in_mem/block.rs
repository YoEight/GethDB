use std::{io, u64};

use bytes::BytesMut;

use geth_mikoshi::InMemoryStorage;

use crate::index::block::BLOCK_ENTRY_SIZE;
use crate::index::ss_table::SsTable;
use crate::index::tests::{in_mem_generate_block, key_of, position_of, revision_of, NUM_OF_KEYS};

#[test]
fn test_in_mem_block_build_single_key() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::new(InMemoryStorage::new(), BLOCK_ENTRY_SIZE);

    table.put_iter(&mut buffer, [(1, 1, 1)])?;

    assert_eq!(1, table.len());

    let block = table.read_block(0)?;
    block.dump();
    let entry = block.try_read(0).unwrap();

    assert_eq!(1, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(1, entry.position);

    Ok(())
}

#[test]
fn test_in_mem_block_build_full() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::new(InMemoryStorage::new(), BLOCK_ENTRY_SIZE);

    table.put_iter(&mut buffer, [(1, 1, 10), (2, 2, 20)])?;

    assert_eq!(2, table.len());

    for idx in 0..table.len() {
        let entry = table.read_block(idx)?.try_read(0).unwrap();

        assert_eq!((idx + 1) as u64, entry.key);
        assert_eq!((idx + 1) as u64, entry.revision);
        assert_eq!(((idx + 1) * 10) as u64, entry.position);
    }

    Ok(())
}

#[test]
fn test_in_mem_block_build_all() -> io::Result<()> {
    let table = in_mem_generate_block();
    let block = table.read_block(0)?;

    for idx in 0..NUM_OF_KEYS {
        let entry = block.try_read(idx).unwrap();

        assert_eq!(key_of(idx), entry.key);
        assert_eq!(revision_of(idx), entry.revision);
        assert_eq!(position_of(idx), entry.position);
    }

    Ok(())
}

#[test]
fn test_in_mem_block_find_key() -> io::Result<()> {
    let table = in_mem_generate_block();
    let block = table.read_block(0)?;

    for i in 0..NUM_OF_KEYS {
        let entry = block
            .find(key_of(i), revision_of(i))
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

    Ok(())
}

#[test]
fn test_in_mem_block_scan_skipped() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::with_default(InMemoryStorage::new());

    table.put_iter(
        &mut buffer,
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
    )?;

    let block = table.read_block(0)?;
    let mut iter = block.scan_forward(2, 0, usize::MAX);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(4, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(6, entry.position);

    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_in_mem_block_scan_skipped_backward() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::with_default(InMemoryStorage::new());

    table.put_iter(
        &mut buffer,
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
    )?;

    let block = table.read_block(0)?;
    let mut iter = block.scan_backward(2, u64::MAX, usize::MAX);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(6, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(4, entry.position);

    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_in_mem_block_scan_forward() -> io::Result<()> {
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

    let block = table.read_block(0)?;
    let mut iter = block.scan_forward(2, 0, usize::MAX);

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

    Ok(())
}

#[test]
fn test_in_mem_block_scan_backward() -> io::Result<()> {
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

    let block = table.read_block(0)?;
    let mut iter = block.scan_backward(2, u64::MAX, usize::MAX);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(6, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(4, entry.position);

    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_in_mem_block_scan_gap() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::with_default(InMemoryStorage::new());

    table.put_iter(&mut buffer, [(2, 1, 5), (2, 2, 6)])?;

    let block = table.read_block(0)?;
    let mut iter = block.scan_forward(2, 0, usize::MAX);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(6, entry.position);

    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_in_mem_block_scan_gap_backward() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::with_default(InMemoryStorage::new());

    table.put_iter(&mut buffer, [(2, 1, 5), (2, 2, 6)])?;

    let block = table.read_block(0)?;
    let mut iter = block.scan_backward(2, u64::MAX, usize::MAX);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(6, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_in_mem_block_scan_not_found_easy_1() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::with_default(InMemoryStorage::new());

    table.put_iter(&mut buffer, [(3, 0, 7), (3, 1, 8), (3, 2, 9)])?;

    let block = table.read_block(0)?;
    let mut iter = block.scan_forward(2, 0, usize::MAX);

    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_in_mem_block_scan_not_found_easy_1_backward() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::with_default(InMemoryStorage::new());

    table.put_iter(&mut buffer, [(3, 0, 7), (3, 1, 8), (3, 2, 9)])?;

    let block = table.read_block(0)?;
    let mut iter = block.scan_backward(2, u64::MAX, usize::MAX);

    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_in_mem_block_scan_not_found_easy_2() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::with_default(InMemoryStorage::new());

    table.put_iter(&mut buffer, [(1, 0, 1), (1, 1, 2), (1, 2, 3)])?;

    let block = table.read_block(0)?;
    let mut iter = block.scan_forward(2, 0, usize::MAX);

    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_in_mem_block_scan_not_found_easy_2_backward() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::with_default(InMemoryStorage::new());

    table.put_iter(&mut buffer, [(1, 0, 1), (1, 1, 2), (1, 2, 3)])?;

    let block = table.read_block(0)?;
    let mut iter = block.scan_backward(2, u64::MAX, usize::MAX);

    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_in_mem_block_scan_not_found_hard() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::with_default(InMemoryStorage::new());

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

    let block = table.read_block(0)?;
    let mut iter = block.scan_forward(2, 0, usize::MAX);

    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_in_mem_block_scan_not_found_hard_backward() -> io::Result<()> {
    let mut buffer = BytesMut::new();
    let mut table = SsTable::with_default(InMemoryStorage::new());

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

    let block = table.read_block(0)?;
    let mut iter = block.scan_backward(2, u64::MAX, usize::MAX);

    assert!(iter.next().is_none());

    Ok(())
}
