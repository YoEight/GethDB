use crate::index::block::BlockEntry;
use crate::index::mem_table::MemTable;
use crate::index::ss_table::SsTable;
use crate::index::{IteratorIO, IteratorIOExt, MergeIO};
use crate::storage::{FileId, Storage};
use bytes::{Buf, BufMut, BytesMut};
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::ops::RangeBounds;
use uuid::Uuid;

pub const LSM_DEFAULT_MEM_TABLE_SIZE: usize = 4_096;
pub const LSM_BASE_SSTABLE_BLOCK_COUNT: usize = 4;

pub fn sst_table_block_count_limit(level: u8) -> usize {
    2 ^ (level as usize) * LSM_BASE_SSTABLE_BLOCK_COUNT
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
pub struct Lsm<S> {
    pub storage: S,
    pub buffer: BytesMut,
    pub settings: LsmSettings,
    pub active_table: MemTable,
    pub logical_position: u64,
    pub immutable_tables: VecDeque<MemTable>,
    pub levels: BTreeMap<u8, VecDeque<SsTable<S>>>,
}

impl<S> Lsm<S>
where
    S: Storage + 'static,
{
    pub fn new(settings: LsmSettings, storage: S) -> Self {
        Self {
            storage,
            buffer: BytesMut::new(),
            settings,
            logical_position: 0,
            active_table: Default::default(),
            immutable_tables: Default::default(),
            levels: Default::default(),
        }
    }

    pub fn with_default(storage: S) -> Self {
        Self::new(Default::default(), storage)
    }

    pub fn load(settings: LsmSettings, storage: S) -> io::Result<Self> {
        let mut levels = BTreeMap::<u8, VecDeque<SsTable<S>>>::new();
        let mut logical_position = 0u64;

        if storage.exists(FileId::IndexMap)? {
            let mut bytes = storage.read_all(FileId::IndexMap)?;

            logical_position = bytes.get_u64_le();
            let _block_size = bytes.get_u32_le();

            // 17 stands for a level byte and an uuid encoded as a 128bits, which is 16bytes.
            while bytes.remaining() >= 17 {
                let level = bytes.get_u8();
                let id = Uuid::from_u128(bytes.get_u128_le());
                let table = SsTable::load(storage.clone(), id)?;

                levels.entry(level).or_default().push_back(table);
            }
        }

        Ok(Lsm {
            storage,
            buffer: BytesMut::default(),
            settings,
            active_table: Default::default(),
            immutable_tables: Default::default(),
            logical_position,
            levels,
        })
    }

    pub fn ss_table_count(&self) -> usize {
        self.levels.values().map(|ts| ts.len()).sum()
    }

    pub fn ss_table_first(&self) -> Option<&SsTable<S>> {
        let ts = self.levels.get(&0)?;
        ts.get(0)
    }

    #[cfg(test)]
    pub fn put_values<V>(&mut self, mut values: V) -> io::Result<()>
    where
        V: IntoIterator<Item = (u64, u64, u64)>,
    {
        for (key, rev, pos) in values {
            self.put(key, rev, pos)?;
        }

        Ok(())
    }

    pub fn put(&mut self, key: u64, revision: u64, position: u64) -> io::Result<()> {
        self.active_table.put(key, revision, position);

        if self.active_table.size() < self.settings.mem_table_max_size {
            return Ok(());
        }

        let mem_table = std::mem::take(&mut self.active_table);
        let mut new_table = SsTable::new(self.storage.clone(), self.settings.base_block_size);

        new_table.put(&mut self.buffer, mem_table.entries().lift())?;

        let mut level = 0u8;
        let mut cleanups = Vec::new();

        loop {
            if let Some(tables) = self.levels.get_mut(&level) {
                if tables.len() + 1 >= self.settings.ss_table_max_count {
                    let mut targets = vec![new_table.iter()];
                    cleanups.push(new_table.id);

                    for table in tables.drain(..) {
                        targets.push(table.iter());
                        cleanups.push(table.id);
                    }

                    let values = MergeIO::new(targets).map(|e| (e.key, e.revision, e.position));

                    new_table = SsTable::new(self.storage.clone(), self.settings.base_block_size);
                    new_table.put(&mut self.buffer, values)?;

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
        self.logical_position = position;

        self.persist()?;

        for id in cleanups {
            self.storage.remove(FileId::SSTable(id))?;
        }

        Ok(())
    }

    pub fn persist(&mut self) -> io::Result<()> {
        self.buffer.put_u64_le(self.logical_position);
        self.buffer.put_u32_le(self.settings.base_block_size as u32);

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

    pub fn get(&self, key: u64, revision: u64) -> io::Result<Option<u64>> {
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

    pub fn scan<R>(&self, key: u64, range: R) -> impl IteratorIO<Item = BlockEntry>
    where
        R: RangeBounds<u64> + Clone + 'static,
    {
        let mut scans: Vec<Box<dyn IteratorIO<Item = BlockEntry>>> = Vec::new();

        scans.push(Box::new(self.active_table.scan(key, range.clone()).lift()));

        for mem_table in self.immutable_tables.iter() {
            scans.push(Box::new(mem_table.scan(key, range.clone()).lift()));
        }

        for tables in self.levels.values() {
            for table in tables {
                scans.push(Box::new(table.scan(key, range.clone())));
            }
        }

        MergeIO::new(scans)
    }

    pub fn highest_revision(&self, key: u64) -> io::Result<Option<u64>> {
        Ok(self.scan(key, ..).last()?.map(|e| e.revision))
    }
}
