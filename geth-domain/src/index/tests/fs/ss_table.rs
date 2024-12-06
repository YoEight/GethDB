use std::path::PathBuf;
use std::{io, u64};

use temp_testdir::TempDir;

use geth_common::IteratorIO;
use geth_mikoshi::FileSystemStorage;

use crate::index::block::get_block_size;
use crate::index::ss_table::SsTable;
use crate::index::tests::{
    fs_generate_stt_with_size, key_of, position_of, revision_of, NUM_OF_KEYS,
};

#[test]
fn test_fs_sst_build_single_key() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let storage = FileSystemStorage::new(root)?;
    let mut table = SsTable::with_capacity(storage, 1);

    table.put_iter([(1, 2, 3)])?;

    let entry = table.find_key(1, 2)?.unwrap();

    assert_eq!(1, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(3, entry.position);

    Ok(())
}

#[test]
fn test_fs_sst_build_two_blocks() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let storage = FileSystemStorage::new(root)?;
    let mut table = SsTable::with_capacity(storage, 1);

    table.put_iter([(1, 2, 3), (2, 3, 4)])?;

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
fn test_fs_sst_key_not_found() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let storage = FileSystemStorage::new(root)?;
    let mut table = SsTable::with_capacity(storage, 1);

    table.put_iter([(1, 2, 3)])?;

    assert!(table.find_key(1, 3)?.is_none());

    Ok(())
}

#[test]
fn test_fs_sst_find_key() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let storage = FileSystemStorage::new(root)?;
    let table = fs_generate_stt_with_size(storage, 1)?;

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
fn test_fs_ss_table_scan() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let storage = FileSystemStorage::new(root)?;
    let mut table = SsTable::with_capacity(storage, 1);

    table.put_iter([
        (1, 0, 1),
        (1, 1, 2),
        (1, 2, 3),
        (2, 0, 4),
        (2, 1, 5),
        (2, 2, 6),
        (3, 0, 7),
        (3, 1, 8),
        (3, 2, 9),
    ])?;

    let mut iter = table.scan_forward(2, 0, usize::MAX);
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
fn test_fs_ss_table_scan_backward() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let storage = FileSystemStorage::new(root)?;
    let mut table = SsTable::with_capacity(storage, 1);

    table.put_iter([
        (1, 0, 1),
        (1, 1, 2),
        (1, 2, 3),
        (2, 0, 4),
        (2, 1, 5),
        (2, 2, 6),
        (3, 0, 7),
        (3, 1, 8),
        (3, 2, 9),
    ])?;

    let mut iter = table.scan_backward(2, u64::MAX, usize::MAX);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(6, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(4, entry.position);

    assert!(iter.next()?.is_none());

    Ok(())
}

#[test]
fn test_fs_ss_table_scan_3_blocks() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let storage = FileSystemStorage::new(root)?;
    let mut table = SsTable::with_capacity(storage, 3);

    table.put_iter([
        (1, 0, 1),
        (1, 1, 2),
        (1, 2, 3),
        (2, 0, 4),
        (2, 1, 5),
        (2, 2, 6),
        (3, 0, 7),
        (3, 1, 8),
        (3, 2, 9),
    ])?;

    assert_eq!(3, table.len());

    let mut iter = table.scan_forward(2, 0, usize::MAX);

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
fn test_fs_ss_table_scan_3_blocks_backward() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let storage = FileSystemStorage::new(root)?;
    let mut table = SsTable::with_capacity(storage, 3);

    table.put_iter([
        (1, 0, 1),
        (1, 1, 2),
        (1, 2, 3),
        (2, 0, 4),
        (2, 1, 5),
        (2, 2, 6),
        (3, 0, 7),
        (3, 1, 8),
        (3, 2, 9),
    ])?;

    assert_eq!(3, table.len());

    let mut iter = table.scan_backward(2, u64::MAX, usize::MAX);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(6, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(4, entry.position);

    assert!(iter.next()?.is_none());

    Ok(())
}

#[test]
fn test_fs_ss_table_scan_not_found() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let storage = FileSystemStorage::new(root)?;
    let mut table = SsTable::with_capacity(storage, 3);

    table.put_iter([
        (1, 0, 1),
        (1, 1, 2),
        (1, 2, 3),
        (3, 0, 7),
        (3, 1, 8),
        (3, 2, 9),
    ])?;

    let mut iter = table.scan_forward(2, 0, usize::MAX);

    assert!(iter.next()?.is_none());

    Ok(())
}

#[test]
fn test_fs_ss_table_scan_not_found_backward() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let storage = FileSystemStorage::new(root)?;
    let mut table = SsTable::with_capacity(storage, 3);

    table.put_iter([
        (1, 0, 1),
        (1, 1, 2),
        (1, 2, 3),
        (3, 0, 7),
        (3, 1, 8),
        (3, 2, 9),
    ])?;

    let mut iter = table.scan_backward(2, u64::MAX, usize::MAX);

    assert!(iter.next()?.is_none());

    Ok(())
}

#[test]
fn test_fs_ss_table_serialization() -> io::Result<()> {
    let temp = TempDir::default();
    let storage = FileSystemStorage::new(PathBuf::from(temp.as_ref()))?;
    let mut table = SsTable::with_capacity(storage.clone(), 10);

    table.put_iter([(1, 2, 3)])?;

    let actual = SsTable::load(storage, table.id)?;

    assert_eq!(get_block_size(10), table.block_size);
    assert_eq!(table.block_size, actual.block_size);
    assert_eq!(table.meta_offset, actual.meta_offset);
    assert_eq!(table.metas, actual.metas);

    Ok(())
}
