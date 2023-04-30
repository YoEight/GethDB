use crate::index::rannoch::block::{BLOCK_ENTRY_SIZE, BLOCK_MIN_SIZE};
use crate::index::rannoch::ss_table::SsTable;
use crate::index::rannoch::storage::fs::FsStorage;
use crate::index::rannoch::storage::in_mem::InMemStorage;
use crate::index::rannoch::tests::fs::values;
use crate::index::rannoch::tests::{
    fs_generate_stt, in_mem_generate_sst, key_of, position_of, revision_of, test_ss_table,
    NUM_OF_KEYS,
};
use crate::index::{IteratorIO, IteratorIOExt};
use bytes::BytesMut;
use std::io;
use std::path::PathBuf;
use temp_testdir::TempDir;
use uuid::Uuid;

#[test]
fn test_fs_sst_build_single_key() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let mut storage = FsStorage::new(root, BLOCK_ENTRY_SIZE);
    let mut table = test_ss_table();

    storage.sst_put(&mut table, values(&[(1, 2, 3)]))?;

    let entry = storage.sst_find_key(&table, 1, 2)?.unwrap();

    assert_eq!(1, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(3, entry.position);

    Ok(())
}

#[test]
fn test_fs_sst_build_two_blocks() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let mut storage = FsStorage::new(root, BLOCK_ENTRY_SIZE);
    let mut table = test_ss_table();

    storage.sst_put(&mut table, values(&[(1, 2, 3), (2, 3, 4)]))?;

    let entry = storage.sst_find_key(&table, 1, 2)?.unwrap();

    assert_eq!(1, entry.key);
    assert_eq!(2, entry.revision);
    assert_eq!(3, entry.position);

    let entry = storage.sst_find_key(&table, 2, 3)?.unwrap();

    assert_eq!(2, entry.key);
    assert_eq!(3, entry.revision);
    assert_eq!(4, entry.position);

    Ok(())
}

#[test]
fn test_fs_sst_key_not_found() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let mut storage = FsStorage::new(root, BLOCK_ENTRY_SIZE);
    let mut table = test_ss_table();

    storage.sst_put(&mut table, values(&[(1, 2, 3)]))?;

    assert!(storage.sst_find_key(&table, 1, 3)?.is_none());

    Ok(())
}

#[test]
fn test_fs_sst_find_key() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let mut storage = FsStorage::new(root, BLOCK_ENTRY_SIZE);
    let table = fs_generate_stt(&mut storage)?;

    for i in 0..NUM_OF_KEYS {
        let key = key_of(i);
        let revision = revision_of(i);
        let position = position_of(i);

        let entry = storage
            .sst_find_key(&table, key, revision)?
            .expect("to be defined");

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
    let mut storage = FsStorage::new(root, BLOCK_ENTRY_SIZE);
    let mut table = SsTable::new();

    storage.sst_put(
        &mut table,
        values(&[
            (1, 0, 1),
            (1, 1, 2),
            (1, 2, 3),
            (2, 0, 4),
            (2, 1, 5),
            (2, 2, 6),
            (3, 0, 7),
            (3, 1, 8),
            (3, 2, 9),
        ]),
    )?;

    let mut iter = storage.sst_scan(&table, 2, ..);

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
fn test_fs_ss_table_scan_3_blocks() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let mut storage = FsStorage::new(root, BLOCK_ENTRY_SIZE * 3);
    let mut table = SsTable::new();

    storage.sst_put(
        &mut table,
        values(&[
            (1, 0, 1),
            (1, 1, 2),
            (1, 2, 3),
            (2, 0, 4),
            (2, 1, 5),
            (2, 2, 6),
            (3, 0, 7),
            (3, 1, 8),
            (3, 2, 9),
        ]),
    )?;

    assert_eq!(3, table.len());

    let mut iter = storage.sst_scan(&table, 2, ..);

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
fn test_fs_ss_table_scan_not_found() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let mut storage = FsStorage::new(root, BLOCK_ENTRY_SIZE * 3);
    let mut table = SsTable::new();

    storage.sst_put(
        &mut table,
        values(&[
            (1, 0, 1),
            (1, 1, 2),
            (1, 2, 3),
            (3, 0, 7),
            (3, 1, 8),
            (3, 2, 9),
        ]),
    )?;

    let mut iter = storage.sst_scan(&table, 2, ..);

    assert!(iter.next()?.is_none());

    Ok(())
}
