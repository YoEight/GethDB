use crate::index::rannoch::mem_table::MemTable;
use bytes::BytesMut;

#[test]
fn test_mem_table_get() {
    let mut mem_table = MemTable::new();

    mem_table.put(1, 1);
    mem_table.put(2, 2);
    mem_table.put(3, 3);

    assert_eq!(1, mem_table.get(1, 0).unwrap());
    assert_eq!(2, mem_table.get(2, 0).unwrap());
    assert_eq!(3, mem_table.get(3, 0).unwrap());
}

#[test]
fn test_mem_table_iter() {
    let mut mem_table = MemTable::new();

    mem_table.put(1, 0);
    mem_table.put(1, 1);
    mem_table.put(1, 2);

    let mut count = 0;
    for (idx, position) in mem_table.scan(1, ..).enumerate() {
        assert_eq!(idx as u64, position);
        count += 1;
    }

    assert_eq!(3, count);

    let mut iter = mem_table.scan(1, 1..);

    assert_eq!(1, iter.next().unwrap());
    assert_eq!(2, iter.next().unwrap());
    assert!(iter.next().is_none());

    let mut iter = mem_table.scan(1, 0..=2);

    assert_eq!(0, iter.next().unwrap());
    assert_eq!(1, iter.next().unwrap());
    assert_eq!(2, iter.next().unwrap());
    assert!(iter.next().is_none());
}

#[test]
fn test_mem_table_flush() {
    let mut buffer = BytesMut::new();
    let mut mem_table = MemTable::new();

    mem_table.put(1, 1);
    mem_table.put(2, 2);
    mem_table.put(3, 3);

    let table = mem_table.flush(&mut buffer, 128);
    let mut iter = table.iter();

    let entry = iter.next().unwrap();
    assert_eq!(1, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(1, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(2, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(2, entry.position);

    let entry = iter.next().unwrap();
    assert_eq!(3, entry.key);
    assert_eq!(0, entry.revision);
    assert_eq!(3, entry.position);

    assert!(iter.next().is_none());
}
