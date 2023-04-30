use crate::index::rannoch::lsm::{Lsm, LsmSettings};
use crate::index::rannoch::mem_table::MEM_TABLE_ENTRY_SIZE;
use crate::index::rannoch::storage::fs::FsStorage;
use crate::index::rannoch::storage::in_mem::InMemStorage;
use crate::index::IteratorIO;
use std::io;
use std::path::PathBuf;
use temp_testdir::TempDir;

fn lsm_put_values<V>(storage: &mut FsStorage, lsm: &mut Lsm, mut values: V) -> io::Result<()>
where
    V: IntoIterator<Item = (u64, u64, u64)>,
{
    for (key, rev, pos) in values {
        storage.lsm_put(lsm, key, rev, pos)?;
    }

    Ok(())
}

#[test]
fn test_fs_lsm_get() -> io::Result<()> {
    let mut lsm = Lsm::default();
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let mut storage = FsStorage::new_with_default(root);

    lsm_put_values(&mut storage, &mut lsm, [(1, 0, 1), (2, 0, 2), (3, 0, 3)])?;

    assert_eq!(1, storage.lsm_get(&lsm, 1, 0)?.unwrap());
    assert_eq!(2, storage.lsm_get(&lsm, 2, 0)?.unwrap());
    assert_eq!(3, storage.lsm_get(&lsm, 3, 0)?.unwrap());

    Ok(())
}

#[test]
fn test_fs_lsm_mem_table_scan() -> io::Result<()> {
    let mut lsm = Lsm::default();
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let mut storage = FsStorage::new_with_default(root);

    lsm_put_values(
        &mut storage,
        &mut lsm,
        [(1, 0, 1), (2, 0, 2), (2, 1, 5), (3, 0, 3)],
    )?;

    let mut iter = storage.lsm_scan(&lsm, 2, ..);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next()?.is_none());

    let mut iter = storage.lsm_scan(&lsm, 2, 0..=2);
    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next()?.is_none());

    let mut iter = storage.lsm_scan(&lsm, 2, 0..2);
    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next()?.is_none());

    let mut iter = storage.lsm_scan(&lsm, 2, 0..=1);
    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next()?.is_none());

    let mut iter = storage.lsm_scan(&lsm, 2, 0..1);
    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    assert!(iter.next()?.is_none());

    let mut iter = storage.lsm_scan(&lsm, 2, 1..);
    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next()?.is_none());

    Ok(())
}

/// Simulates compaction by purposely using tiny memtable size.
/// When scanning, it will access to sstables.
#[test]
fn test_fs_lsm_sync() -> io::Result<()> {
    let mut setts = LsmSettings::default();
    setts.mem_table_max_size = MEM_TABLE_ENTRY_SIZE;

    let mut lsm = Lsm::new(setts);
    let temp = TempDir::default();
    let root = PathBuf::from(temp.as_ref());
    let mut storage = FsStorage::new_with_default(root);

    lsm_put_values(
        &mut storage,
        &mut lsm,
        [(1, 0, 1), (2, 0, 2), (2, 1, 5), (3, 0, 3)],
    )?;

    assert_eq!(1, lsm.ss_table_count());
    let table = lsm.ss_table_first().unwrap();
    let block = storage.sst_read_block(table, 0)?.unwrap();
    block.dump();

    let mut iter = storage.lsm_scan(&lsm, 2, ..);

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
