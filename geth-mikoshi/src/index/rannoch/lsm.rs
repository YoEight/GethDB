use crate::index::rannoch::mem_table::{MemTable, MEM_TABLE_ENTRY_SIZE};
use crate::index::rannoch::ss_table::SsTable;
use bytes::BytesMut;
use std::collections::BTreeMap;
use std::ops::RangeBounds;

pub const LSM_DEFAULT_MEM_TABLE_SIZE: usize = 4_096;
pub const LSM_BASE_SSTABLE_BLOCK_COUNT: usize = 4;

pub struct LsmStorage {
    pub mem_table_max_size: usize,
    pub ss_table_max_count: usize,
    pub active_table: MemTable,
    pub immutable_tables: Vec<MemTable>,
    pub levels: BTreeMap<u8, Vec<SsTable>>,
}

impl LsmStorage {
    pub fn empty_with_default() -> Self {
        Self {
            mem_table_max_size: LSM_DEFAULT_MEM_TABLE_SIZE,
            ss_table_max_count: 4,
            active_table: MemTable::default(),
            immutable_tables: vec![],
            levels: Default::default(),
        }
    }

    pub fn put(&mut self, key: u64, revision: u64, position: u64) {
        if self.active_table.size() + MEM_TABLE_ENTRY_SIZE > self.mem_table_max_size {
            self.bookkeeping();
        }

        self.active_table.put(key, revision, position);
    }

    pub fn get(&self, key: u64, revision: u64) -> Option<u64> {
        todo!()
    }

    pub fn scan<R>(&self, key: u64, range: R)
    where
        R: RangeBounds<u64>,
    {
        todo!()
    }

    fn bookkeeping(&mut self) {}
}
