use std::io;

use crate::index::merge::Merge;
use crate::index::tests::{build_mem_table, check_merge_io_result};

#[test]
fn test_merge_io_mem_table_1() -> io::Result<()> {
    let mut builder = Merge::builder_for_mem_tables_only();
    let mem_1 = build_mem_table([(1, 0, 1), (2, 0, 2), (3, 0, 3)]);
    let mem_2 = build_mem_table([(1, 0, 2), (2, 0, 4), (3, 0, 6), (4, 0, 8)]);
    let mem_3 = build_mem_table([(2, 0, 12), (3, 0, 18), (4, 0, 24)]);

    builder.push_mem_table_scan(mem_1.clone().into_iter());
    builder.push_mem_table_scan(mem_2.clone().into_iter());
    builder.push_mem_table_scan(mem_3.clone().into_iter());

    let merge_iter = builder.build();

    check_merge_io_result(merge_iter, [(1, 0, 1), (2, 0, 2), (3, 0, 3), (4, 0, 8)])?;

    let mut builder = Merge::builder_for_mem_tables_only();

    builder.push_mem_table_scan(mem_3.clone().into_iter());
    builder.push_mem_table_scan(mem_1.clone().into_iter());
    builder.push_mem_table_scan(mem_2.clone().into_iter());

    let merge_iter = builder.build();

    check_merge_io_result(merge_iter, [(1, 0, 1), (2, 0, 12), (3, 0, 18), (4, 0, 24)])?;

    Ok(())
}

#[test]
fn test_merge_io_mem_table_2() -> io::Result<()> {
    let mut builder = Merge::builder_for_mem_tables_only();
    let mem_1 = build_mem_table([(1, 0, 11), (2, 0, 12), (3, 0, 13)]);
    let mem_2 = build_mem_table([(4, 0, 21), (5, 0, 22), (6, 0, 23), (7, 0, 24)]);
    let mem_3 = build_mem_table([(8, 0, 31), (9, 0, 32), (10, 0, 33), (11, 0, 34)]);
    let mem_4 = build_mem_table(vec![]);

    builder.push_mem_table_scan(mem_1.clone().into_iter());
    builder.push_mem_table_scan(mem_2.clone().into_iter());
    builder.push_mem_table_scan(mem_3.clone().into_iter());
    builder.push_mem_table_scan(mem_4.clone().into_iter());

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

    check_merge_io_result(builder.build(), result.clone())?;

    let mut builder = Merge::builder_for_mem_tables_only();
    builder.push_mem_table_scan(mem_2.clone().into_iter());
    builder.push_mem_table_scan(mem_4.clone().into_iter());
    builder.push_mem_table_scan(mem_3.clone().into_iter());
    builder.push_mem_table_scan(mem_1.clone().into_iter());

    check_merge_io_result(builder.build(), result.clone())?;

    let mut builder = Merge::builder_for_mem_tables_only();
    builder.push_mem_table_scan(mem_4.clone().into_iter());
    builder.push_mem_table_scan(mem_3.clone().into_iter());
    builder.push_mem_table_scan(mem_2.clone().into_iter());
    builder.push_mem_table_scan(mem_1.clone().into_iter());

    check_merge_io_result(builder.build(), result.clone())?;

    Ok(())
}
