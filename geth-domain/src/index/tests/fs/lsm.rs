use std::io;
use std::path::PathBuf;

use bytes::BytesMut;
use temp_testdir::TempDir;

use geth_common::{Direction, IteratorIO, Revision};
use geth_mikoshi::FileSystemStorage;

use crate::index::lsm::{Lsm, LsmSettings};
use crate::index::mem_table::MEM_TABLE_ENTRY_SIZE;
use crate::index::ss_table::SsTable;

#[test]
fn test_fs_lsm_get() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let mut lsm = Lsm::with_default(FileSystemStorage::new(root)?);

    lsm.put_values([(1, 0, 1), (2, 0, 2), (3, 0, 3)])?;

    assert_eq!(1, lsm.get(1, 0)?.unwrap());
    assert_eq!(2, lsm.get(2, 0)?.unwrap());
    assert_eq!(3, lsm.get(3, 0)?.unwrap());

    Ok(())
}

#[test]
fn test_fs_lsm_mem_table_scan() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let lsm = Lsm::with_default(FileSystemStorage::new(root)?);

    lsm.put_values([(1, 0, 1), (2, 0, 2), (2, 1, 5), (3, 0, 3)])?;

    let mut iter = lsm.scan_forward(2, 0, usize::MAX);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next()?.is_none());

    let mut iter = lsm.scan_forward(2, 0, 2);
    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next()?.is_none());

    let mut iter = lsm.scan_forward(2, 0, 1);
    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    assert!(iter.next()?.is_none());

    Ok(())
}

/// Simulates compaction by purposely using tiny memtable size.
/// When scanning, it will access to sstables.
#[test]
fn test_fs_lsm_sync() -> io::Result<()> {
    let mut setts = LsmSettings::default();
    setts.mem_table_max_size = MEM_TABLE_ENTRY_SIZE;

    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let storage = FileSystemStorage::new(root)?;
    let mut lsm = Lsm::new(setts, storage);

    lsm.put_values([(1, 0, 1), (2, 0, 2), (2, 1, 5), (3, 0, 3)])?;

    assert_eq!(1, lsm.ss_table_count());
    let table = lsm.ss_table_first().unwrap();
    let block = table.read_block(0)?;
    block.dump();

    let mut iter = lsm.scan_forward(2, 0, usize::MAX);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next()?.is_none());

    Ok(())
}

#[test]
fn test_fs_lsm_serialization() -> io::Result<()> {
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let storage = FileSystemStorage::new(root)?;
    let mut buffer = BytesMut::new();
    let mut lsm = Lsm::with_default(storage.clone());

    let mut table1 = SsTable::with_default(storage.clone());
    let mut table2 = SsTable::with_default(storage.clone());

    table1.put_iter(&mut buffer, [(1, 2, 3)])?;
    table2.put_iter(&mut buffer, [(4, 5, 6)])?;

    lsm.logical_position = 1234;

    {
        lsm.levels.entry(0).or_default().push_back(table1.clone());
    }

    {
        lsm.levels.entry(1).or_default().push_back(table2.clone());
    }

    lsm.persist()?;
    let actual = Lsm::load(LsmSettings::default(), storage)?;

    assert_eq!(lsm.logical_position, actual.logical_position);

    let actual_table_1 = actual.levels.get(&0).unwrap().front().clone().unwrap();
    let actual_table_2 = actual.levels.get(&1).unwrap().front().clone().unwrap();

    assert_eq!(table1.id, actual_table_1.id);
    assert_eq!(table1.metas, actual_table_1.metas);
    assert_eq!(table2.id, actual_table_2.id);
    assert_eq!(table2.metas, actual_table_2.metas);

    Ok(())
}
