use crate::index::rannoch::mem_table::{MemTable, MEM_TABLE_ENTRY_SIZE};
use crate::index::rannoch::ss_table::SsTable;
use bytes::BytesMut;
use std::collections::{BTreeMap, VecDeque};
use std::ops::RangeBounds;

pub const LSM_DEFAULT_MEM_TABLE_SIZE: usize = 4_096;
pub const LSM_BASE_SSTABLE_BLOCK_COUNT: usize = 4;

pub fn sst_table_block_count_limit(level: u8) -> usize {
    2 ^ (level as usize) * LSM_BASE_SSTABLE_BLOCK_COUNT
}

#[derive(Debug, Clone, Copy)]
pub struct LsmSettings {
    pub mem_table_max_size: usize,
    pub ss_table_max_count: usize,
}

impl Default for LsmSettings {
    fn default() -> Self {
        Self {
            mem_table_max_size: LSM_DEFAULT_MEM_TABLE_SIZE,
            ss_table_max_count: LSM_BASE_SSTABLE_BLOCK_COUNT,
        }
    }
}

#[derive(Default)]
pub struct Lsm {
    pub settings: LsmSettings,
    pub active_table: MemTable,
    pub immutable_tables: VecDeque<MemTable>,
    pub levels: BTreeMap<u8, VecDeque<SsTable>>,
}

impl Lsm {
    pub fn new(settings: LsmSettings) -> Self {
        Self {
            settings,
            active_table: Default::default(),
            immutable_tables: Default::default(),
            levels: Default::default(),
        }
    }

    pub fn ss_table_count(&self) -> usize {
        self.levels.values().map(|ts| ts.len()).sum()
    }

    pub fn ss_table_first(&self) -> Option<&SsTable> {
        let ts = self.levels.get(&0)?;
        ts.get(0)
    }
}
