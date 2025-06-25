use std::io;

use geth_common::IteratorIO;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::InMemoryStorage;

use crate::index::block::BlockEntry;
use crate::index::mem_table::MemTable;
use crate::index::merge::Merge;
use crate::index::ss_table::SsTable;

mod fs;
mod in_mem;

pub const NUM_OF_KEYS: usize = 100;

pub fn key_of(idx: usize) -> u64 {
    idx as u64 * 5
}

pub fn revision_of(idx: usize) -> u64 {
    idx as u64 * 42
}

pub fn position_of(idx: usize) -> u64 {
    idx as u64
}

pub fn in_mem_generate_block() -> SsTable {
    let mut table = SsTable::new(InMemoryStorage::new(), 4_096);
    let values = (0..NUM_OF_KEYS).map(|idx| (key_of(idx), revision_of(idx), position_of(idx)));
    table.put_iter(values).unwrap();

    table
}

pub fn in_mem_generate_sst() -> SsTable {
    let mut table = SsTable::new(InMemoryStorage::new(), 128);
    let values = (0..NUM_OF_KEYS).map(|idx| (key_of(idx), revision_of(idx), position_of(idx)));
    table.put_iter(values).unwrap();

    table
}

pub fn fs_generate_stt_with_size(storage: Storage, num_elems: usize) -> io::Result<SsTable> {
    let mut table = SsTable::with_capacity(storage, num_elems);
    let values = (0..NUM_OF_KEYS).map(|idx| (key_of(idx), revision_of(idx), position_of(idx)));
    table.put_iter(values)?;

    Ok(table)
}

pub fn build_mem_table<Values>(inputs: Values) -> MemTable
where
    Values: IntoIterator<Item = (u64, u64, u64)>,
{
    let mut mem_table = MemTable::default();

    for (key, revision, position) in inputs {
        mem_table.put(key, revision, position);
    }

    mem_table
}

pub fn check_merge_io_result<TMemTable, TSSTable, Values>(
    mut target: Merge<TMemTable, TSSTable>,
    expecteds: Values,
) -> io::Result<()>
where
    TMemTable: Iterator<Item = BlockEntry>,
    TSSTable: IteratorIO<Item = BlockEntry>,
    Values: IntoIterator<Item = (u64, u64, u64)>,
{
    for (key, revision, position) in expecteds {
        let actual = target.next()?.unwrap();
        assert_eq!(
            BlockEntry {
                key,
                revision,
                position,
            },
            actual
        );
    }

    Ok(())
}
