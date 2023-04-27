use crate::index::rannoch::in_mem::InMemStorage;
use crate::index::rannoch::lsm::{Lsm, LsmSettings};
use crate::index::rannoch::mem_table::MEM_TABLE_ENTRY_SIZE;

fn lsm_put_values<V>(storage: &mut InMemStorage, lsm: &mut Lsm, mut values: V)
where
    V: IntoIterator<Item = (u64, u64, u64)>,
{
    for (key, rev, pos) in values {
        storage.lsm_put(lsm, key, rev, pos);
    }
}

#[test]
fn test_in_mem_lsm_get() {
    let mut lsm = Lsm::default();
    let mut storage = InMemStorage::default();

    lsm_put_values(&mut storage, &mut lsm, [(1, 0, 1), (2, 0, 2), (3, 0, 3)]);

    assert_eq!(1, storage.lsm_get(&lsm, 1, 0).unwrap());
    assert_eq!(2, storage.lsm_get(&lsm, 2, 0).unwrap());
    assert_eq!(3, storage.lsm_get(&lsm, 3, 0).unwrap());
}

#[test]
fn test_in_mem_lsm_mem_table_scan() {
    let mut lsm = Lsm::default();
    let mut storage = InMemStorage::default();

    lsm_put_values(
        &mut storage,
        &mut lsm,
        [(1, 0, 1), (2, 0, 2), (2, 1, 5), (3, 0, 3)],
    );

    let mut iter = storage.lsm_scan(&lsm, 2, ..);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next().is_none());

    let mut iter = storage.lsm_scan(&lsm, 2, 0..=2);
    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next().is_none());

    let mut iter = storage.lsm_scan(&lsm, 2, 0..2);
    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next().is_none());

    let mut iter = storage.lsm_scan(&lsm, 2, 0..=1);
    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next().is_none());

    let mut iter = storage.lsm_scan(&lsm, 2, 0..1);
    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    assert!(iter.next().is_none());

    let mut iter = storage.lsm_scan(&lsm, 2, 1..);
    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next().is_none());
}

/// Simulates compaction by purposely using tiny memtable size.
/// When scanning, it will access to in-mem sstables.
#[test]
fn test_in_mem_lsm_sync() {
    let mut setts = LsmSettings::default();
    setts.mem_table_max_size = MEM_TABLE_ENTRY_SIZE;

    let mut lsm = Lsm::new(setts);
    let mut storage = InMemStorage::default();

    lsm_put_values(
        &mut storage,
        &mut lsm,
        [(1, 0, 1), (2, 0, 2), (2, 1, 5), (3, 0, 3)],
    );

    assert_eq!(1, lsm.ss_table_count());
    let table = lsm.ss_table_first().unwrap();
    let block = storage.sst_read_block(table, 0).unwrap();
    block.dump();

    let mut iter = storage.lsm_scan(&lsm, 2, ..);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(1, entry.revision);
    assert_eq!(5, entry.position);

    assert!(iter.next().is_none());
}
