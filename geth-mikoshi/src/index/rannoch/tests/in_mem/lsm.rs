use crate::index::rannoch::lsm::{Lsm, LsmSettings};
use crate::index::rannoch::mem_table::MEM_TABLE_ENTRY_SIZE;
use crate::index::rannoch::storage::in_mem::InMemStorage;
use crate::storage::in_mem::InMemoryStorage;
use std::io;

#[test]
fn test_in_mem_lsm_get() -> io::Result<()> {
    let mut lsm = Lsm::with_default(InMemoryStorage::new());

    lsm.put_values([(1, 0, 1), (2, 0, 2), (3, 0, 3)])?;

    assert_eq!(1, lsm.get(1, 0)?.unwrap());
    assert_eq!(2, lsm.get(2, 0)?.unwrap());
    assert_eq!(3, lsm.get(3, 0)?.unwrap());

    Ok(())
}

#[test]
fn test_in_mem_lsm_mem_table_scan() -> io::Result<()> {
    let mut lsm = Lsm::with_default(InMemoryStorage::new());

    lsm.put_values([(1, 0, 1), (2, 0, 2), (2, 1, 5), (3, 0, 3)])?;

    let mut iter = lsm.scan(2, ..);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next()?.is_none());

    let mut iter = lsm.scan(2, 0..=2);
    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next()?.is_none());

    let mut iter = lsm.scan(2, 0..2);
    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next()?.is_none());

    let mut iter = lsm.scan(2, 0..=1);
    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next()?.is_none());

    let mut iter = lsm.scan(2, 0..1);
    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    assert!(iter.next()?.is_none());

    let mut iter = lsm.scan(2, 1..);
    let entry = iter.next()?.unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next()?.is_none());

    Ok(())
}

/// Simulates compaction by purposely using tiny memtable size.
/// When scanning, it will access to in-mem sstables.
#[test]
fn test_in_mem_lsm_sync() -> io::Result<()> {
    let mut setts = LsmSettings::default();
    setts.mem_table_max_size = MEM_TABLE_ENTRY_SIZE;

    let mut lsm = Lsm::new(setts, InMemoryStorage::new());

    lsm.put_values([(1, 0, 1), (2, 0, 2), (2, 1, 5), (3, 0, 3)])?;

    assert_eq!(1, lsm.ss_table_count());
    let table = lsm.ss_table_first().unwrap();
    let block = table.read_block(0)?;
    block.dump();

    let mut iter = lsm.scan(2, ..);

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
