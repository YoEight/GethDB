use crate::index::rannoch::ss_table::{BlockMetas, SsTable};
use crate::index::rannoch::storage::fs::FsStorage;
use crate::index::rannoch::storage::in_mem::InMemStorage;
use crate::index::IteratorIOExt;
use std::io;
use std::path::PathBuf;
use uuid::Uuid;

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

pub fn test_ss_table() -> SsTable {
    SsTable {
        id: Uuid::nil(),
        metas: BlockMetas::new(Default::default()),
    }
}

pub fn in_mem_generate_block(storage: &mut InMemStorage) {
    let mut table = test_ss_table();
    let values = (0..NUM_OF_KEYS).map(|idx| (key_of(idx), revision_of(idx), position_of(idx)));
    storage.sst_put(&mut table, values);
}

pub fn in_mem_generate_sst() -> InMemStorage {
    let mut storage = InMemStorage::new(128);
    in_mem_generate_block(&mut storage);

    storage
}

pub fn fs_generate_stt(storage: &mut FsStorage) -> io::Result<SsTable> {
    let mut table = test_ss_table();
    let values = (0..NUM_OF_KEYS).map(|idx| (key_of(idx), revision_of(idx), position_of(idx)));
    storage.sst_put(&mut table, values.lift())?;

    Ok(table)
}
