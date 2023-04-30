use crate::index::rannoch::block::BlockEntry;
use crate::index::rannoch::mem_table::{MemTable, Scan};
use crate::index::rannoch::merge::Merge;
use crate::index::rannoch::tests::{build_mem_table, check_merge_result};
use std::ops::RangeFull;

#[test]
fn test_merge_mem_table_1() {
    let mem_1 = build_mem_table([(1, 0, 1), (2, 0, 2), (3, 0, 3)]);
    let mem_2 = build_mem_table([(1, 0, 2), (2, 0, 4), (3, 0, 6), (4, 0, 8)]);
    let mem_3 = build_mem_table([(2, 0, 12), (3, 0, 18), (4, 0, 24)]);

    let iters = vec![
        mem_1.clone().into_iter(),
        mem_2.clone().into_iter(),
        mem_3.clone().into_iter(),
    ];

    let merge_iter = Merge::new(iters);

    check_merge_result(merge_iter, [(1, 0, 1), (2, 0, 2), (3, 0, 3), (4, 0, 8)]);

    let iters = vec![mem_3.into_iter(), mem_1.into_iter(), mem_2.into_iter()];
    let mut merge_iter = Merge::new(iters);

    check_merge_result(merge_iter, [(1, 0, 1), (2, 0, 12), (3, 0, 18), (4, 0, 24)]);
}

#[test]
fn test_merge_mem_table_2() {
    let mem_1 = build_mem_table([(1, 0, 11), (2, 0, 12), (3, 0, 13)]);
    let mem_2 = build_mem_table([(4, 0, 21), (5, 0, 22), (6, 0, 23), (7, 0, 24)]);
    let mem_3 = build_mem_table([(8, 0, 31), (9, 0, 32), (10, 0, 33), (11, 0, 34)]);
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

    check_merge_result(merge_iter, result.clone());

    let mut merge_iter = Merge::new(vec![
        mem_2.clone().into_iter(),
        mem_4.clone().into_iter(),
        mem_3.clone().into_iter(),
        mem_1.clone().into_iter(),
    ]);

    check_merge_result(merge_iter, result.clone());

    let mut merge_iter = Merge::new(vec![
        mem_4.clone().into_iter(),
        mem_3.clone().into_iter(),
        mem_2.clone().into_iter(),
        mem_1.clone().into_iter(),
    ]);

    check_merge_result(merge_iter, result.clone());
}
