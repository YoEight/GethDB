use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::iter::once;

use bytes::{Buf, BufMut, BytesMut};
use uuid::Uuid;

use crate::index::block::BlockEntry;
use crate::index::mem_table::MemTable;
use crate::index::merge::Merge;
use crate::index::ss_table::SsTable;
use geth_common::{IteratorIO, IteratorIOExt};
use geth_mikoshi::storage::{FileId, Storage};

pub const LSM_DEFAULT_MEM_TABLE_SIZE: usize = 4_096;
pub const LSM_BASE_SSTABLE_BLOCK_COUNT: usize = 4;

pub fn sst_table_block_count_limit(level: u8) -> usize {
    (2 ^ (level as usize)) * LSM_BASE_SSTABLE_BLOCK_COUNT
}

#[derive(Debug, Clone, Copy)]
pub struct LsmSettings {
    pub mem_table_max_size: usize,
    pub ss_table_max_count: usize,
    pub base_block_size: usize,
}

impl Default for LsmSettings {
    fn default() -> Self {
        Self {
            mem_table_max_size: LSM_DEFAULT_MEM_TABLE_SIZE,
            ss_table_max_count: LSM_BASE_SSTABLE_BLOCK_COUNT,
            base_block_size: 4_096,
        }
    }
}

#[derive(Clone)]
pub struct Lsm {
    pub storage: Storage,
    pub buffer: BytesMut,
    pub settings: LsmSettings,
    pub active_table: MemTable,
    pub logical_position: u64,
    pub immutable_tables: VecDeque<MemTable>,
    pub levels: BTreeMap<u8, VecDeque<SsTable>>,
}

impl Lsm {
    pub fn new(settings: LsmSettings, storage: Storage) -> Self {
        Self {
            storage,
            buffer: BytesMut::new(),
            settings,
            active_table: Default::default(),
            logical_position: 0,
            immutable_tables: Default::default(),
            levels: Default::default(),
        }
    }

    pub fn with_default(storage: Storage) -> Self {
        Self::new(Default::default(), storage)
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub fn load(settings: LsmSettings, storage: Storage) -> io::Result<Self> {
        let mut lsm = Lsm::new(settings, storage.clone());

        if storage.exists(FileId::IndexMap)? {
            let mut bytes = storage.read_all(FileId::IndexMap)?;

            lsm.logical_position = bytes.get_u64_le();

            // 17 stands for a level byte and an uuid encoded as a 128bits, which is 16bytes.
            while bytes.remaining() >= 17 {
                let level = bytes.get_u8();
                let id = Uuid::from_u128(bytes.get_u128_le());
                let table = SsTable::load_with_buffer(storage.clone(), id, lsm.buffer.split())?;

                lsm.levels.entry(level).or_default().push_back(table);
            }
        }

        Ok(lsm)
    }

    pub fn ss_table_count(&self) -> usize {
        self.levels.values().map(|ts| ts.len()).sum()
    }

    pub fn ss_table_first(&self) -> Option<SsTable> {
        self.levels.get(&0)?.front().cloned()
    }

    pub fn put_values<V>(&mut self, values: V) -> io::Result<()>
    where
        V: IntoIterator<Item = (u64, u64, u64)>,
    {
        self.put(values.into_iter().lift())
    }

    pub fn put_single(&mut self, key: u64, revision: u64, position: u64) -> io::Result<()> {
        self.put_values(once((key, revision, position)))
    }

    pub fn put<Values>(&mut self, mut values: Values) -> io::Result<()>
    where
        Values: IteratorIO<Item = (u64, u64, u64)>,
    {
        while let Some((key, revision, position)) = values.next()? {
            self.active_table.put(key, revision, position);
            // TODO - we shouldn't update the logical position when pushing to memtables. We must
            // update logical_position only when flushing entries to ss_tables.
            self.logical_position = position;
        }

        if self.active_table.size() < self.settings.mem_table_max_size {
            return Ok(());
        }

        let mem_table = std::mem::take(&mut self.active_table);
        let mut new_table = SsTable::with_buffer(
            self.storage.clone(),
            self.settings.base_block_size,
            self.buffer.split(),
        );

        new_table.put(mem_table.entries().lift())?;

        let mut level = 0u8;
        let mut cleanups = Vec::new();

        loop {
            if let Some(tables) = self.levels.get_mut(&level) {
                if tables.len() + 1 >= self.settings.ss_table_max_count {
                    let mut builder = Merge::builder_for_ss_tables_only();
                    cleanups.push(new_table.id);

                    for table in tables.drain(..) {
                        builder.push_ss_table_scan(table.iter());
                        cleanups.push(table.id);
                    }

                    let values = builder.build().map(|e| (e.key, e.revision, e.position));

                    new_table = SsTable::new(self.storage.clone(), self.settings.base_block_size);
                    new_table.put(values)?;

                    if new_table.len() >= sst_table_block_count_limit(level) {
                        level += 1;
                        continue;
                    }
                }

                tables.push_front(new_table);
                break;
            }

            let mut tables = VecDeque::new();
            tables.push_front(new_table);

            self.levels.insert(level, tables);
            break;
        }

        // We only update the logical position this late because if we went beyond the main loop,
        // it means we actually flushed some data to disk. Anything prior is stored in mem-table.
        self.persist()?;

        for id in cleanups {
            self.storage.remove(FileId::SSTable(id))?;
        }

        Ok(())
    }

    pub fn get(&mut self, key: u64, revision: u64) -> io::Result<Option<u64>> {
        let mut result = self.active_table.get(key, revision);

        if result.is_some() {
            return Ok(result);
        }

        for mem_table in self.immutable_tables.iter() {
            result = mem_table.get(key, revision);

            if result.is_some() {
                return Ok(result);
            }
        }

        for ss_tables in self.levels.values() {
            for table in ss_tables {
                result = table.find_key(key, revision)?.map(|e| e.position);

                if result.is_some() {
                    return Ok(result);
                }
            }
        }

        Ok(None)
    }

    pub fn scan_forward(
        &self,
        key: u64,
        start: u64,
        count: usize,
    ) -> impl IteratorIO<Item = BlockEntry> + use<'_> {
        let mut builder = Merge::builder();

        builder.push_mem_table_scan(self.active_table.scan_forward(key, start, count));

        for mem_table in self.immutable_tables.iter() {
            builder.push_mem_table_scan(mem_table.scan_forward(key, start, count));
        }

        for tables in self.levels.values() {
            for table in tables {
                builder.push_ss_table_scan(table.scan_forward(key, start, count));
            }
        }

        builder.build()
    }

    pub fn scan_backward(
        &self,
        key: u64,
        start: u64,
        count: usize,
    ) -> impl IteratorIO<Item = BlockEntry> + use<'_> {
        let mut builder = Merge::builder();

        builder.push_mem_table_scan(self.active_table.scan_backward(key, start, count));

        for mem_table in self.immutable_tables.iter() {
            builder.push_mem_table_scan(mem_table.scan_backward(key, start, count));
        }

        for tables in self.levels.values() {
            for table in tables {
                builder.push_ss_table_scan(table.scan_backward(key, start, count));
            }
        }

        builder.build()
    }

    pub fn highest_revision(&self, key: u64) -> io::Result<Option<u64>> {
        Ok(self
            .scan_backward(key, u64::MAX, 1)
            .last()?
            .map(|e| e.revision))
    }

    pub(crate) fn persist(&mut self) -> io::Result<()> {
        self.buffer.put_u64_le(self.logical_position);

        for (level, tables) in &self.levels {
            for table in tables {
                self.buffer.put_u8(*level);
                self.buffer.put_u128_le(table.id.as_u128());
            }
        }

        self.storage
            .write_to(FileId::IndexMap, 0, self.buffer.split().freeze())?;

        Ok(())
    }
}
