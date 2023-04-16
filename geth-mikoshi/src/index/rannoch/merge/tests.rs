use crate::index::rannoch::block::BlockEntry;
use crate::index::rannoch::mem_table::{MemTable, Scan};
use crate::index::rannoch::merge::Merge;
use std::ops::RangeFull;

fn build_mem_table(inputs: Vec<(u64, u64, u64)>) -> MemTable {
    let mut mem_table = MemTable::new();

    for (key, revision, position) in inputs {
        mem_table.put(key, revision, position);
    }

    mem_table
}

fn check_result<I>(mut target: Merge<I>, expecteds: Vec<(u64, u64, u64)>)
where
    I: Iterator<Item = BlockEntry>,
{
    for (key, revision, position) in expecteds {
        let actual = target.next().unwrap();
        assert_eq!(
            BlockEntry {
                key,
                revision,
                position,
            },
            actual
        );
    }
}

#[test]
fn test_merge_mem_table_1() {
    let mem_1 = build_mem_table(vec![(1, 0, 1), (2, 0, 2), (3, 0, 3)]);
    let mem_2 = build_mem_table(vec![(1, 0, 2), (2, 0, 4), (3, 0, 6), (4, 0, 8)]);
    let mem_3 = build_mem_table(vec![(2, 0, 12), (3, 0, 18), (4, 0, 24)]);

    let iters = vec![
        mem_1.clone().into_iter(),
        mem_2.clone().into_iter(),
        mem_3.clone().into_iter(),
    ];

    let merge_iter = Merge::new(iters);

    check_result(merge_iter, vec![(1, 0, 1), (2, 0, 2), (3, 0, 3), (4, 0, 8)]);

    let iters = vec![mem_3.into_iter(), mem_1.into_iter(), mem_2.into_iter()];
    let mut merge_iter = Merge::new(iters);

    check_result(
        merge_iter,
        vec![(1, 0, 1), (2, 0, 12), (3, 0, 18), (4, 0, 24)],
    );
}

#[test]
fn test_merge_mem_table_2() {
    let mem_1 = build_mem_table(vec![(1, 0, 11), (2, 0, 12), (3, 0, 13)]);
    let mem_2 = build_mem_table(vec![(4, 0, 21), (5, 0, 22), (6, 0, 23), (7, 0, 24)]);
    let mem_3 = build_mem_table(vec![(8, 0, 31), (9, 0, 32), (10, 0, 33), (11, 0, 34)]);
    let mem_4 = build_mem_table(vec![]);

    let mut merge_iter = Merge::new(vec![
        mem_1.clone().into_iter(),
        mem_2.clone().into_iter(),
        mem_3.clone().into_iter(),
        mem_4.clone().into_iter(),
    ]);

    let result = vec![
        (1, 0, 11),
        (2, 0, 12),
        (3, 0, 13),
        (4, 0, 21),
        (5, 0, 22),
        (6, 0, 23),
        (7, 0, 24),
        (8, 0, 31),
        (9, 0, 32),
        (10, 0, 33),
        (11, 0, 34),
    ];

    check_result(merge_iter, result.clone());

    let mut merge_iter = Merge::new(vec![
        mem_2.clone().into_iter(),
        mem_4.clone().into_iter(),
        mem_3.clone().into_iter(),
        mem_1.clone().into_iter(),
    ]);

    check_result(merge_iter, result.clone());

    let mut merge_iter = Merge::new(vec![
        mem_4.clone().into_iter(),
        mem_3.clone().into_iter(),
        mem_2.clone().into_iter(),
        mem_1.clone().into_iter(),
    ]);

    check_result(merge_iter, result.clone());
    /*    check_result(merge_iter, vec![(1, 0, 1), (2, 0, 2), (3, 0, 3), (4, 0, 8)]);

    let iters = vec![mem_3.into_iter(), mem_1.into_iter(), mem_2.into_iter()];
    let mut merge_iter = Merge::new(iters);

    check_result(
        merge_iter,
        vec![(1, 0, 1), (2, 0, 12), (3, 0, 18), (4, 0, 24)],
    );*/
}
