use crate::hashing::mikoshi_hash;
use crate::index::block::BlockEntry;
use crate::index::mem_table::MemTable;
use crate::index::ss_table::SsTable;
use crate::index::{IteratorIO, IteratorIOExt, Merge};
use crate::storage::{FileId, Storage};
use crate::wal::ChunkManager;
use bytes::{Buf, BufMut, BytesMut};
use geth_common::{Direction, Revision};
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::iter::once;
use std::sync::{Arc, RwLock};
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

#[derive(Debug)]
pub(crate) struct State<S> {
    pub active_table: MemTable,
    pub logical_position: u64,
    pub immutable_tables: VecDeque<MemTable>,
    pub levels: BTreeMap<u8, VecDeque<SsTable<S>>>,
}

impl<S> Default for State<S> {
    fn default() -> Self {
        Self {
            active_table: Default::default(),
            logical_position: 0,
            immutable_tables: Default::default(),
            levels: Default::default(),
        }
    }
}

impl<S> State<S>
where
    S: Storage,
{
    pub(crate) fn persist(&mut self, buffer: &mut BytesMut, storage: &S) -> io::Result<()> {
        buffer.put_u64_le(self.logical_position);

        for (level, tables) in &self.levels {
            for table in tables {
                buffer.put_u8(*level);
                buffer.put_u128_le(table.id.as_u128());
            }
        }

        storage.write_to(FileId::IndexMap, 0, buffer.split().freeze())?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct Lsm<S> {
    pub storage: S,
    pub buffer: BytesMut,
    pub settings: LsmSettings,
    pub(crate) state: Arc<RwLock<State<S>>>,
}

impl<S> Lsm<S>
where
    S: Storage + Send + Sync + 'static,
{
    pub fn new(settings: LsmSettings, storage: S) -> Self {
        Self {
            storage,
            buffer: BytesMut::new(),
            settings,
            state: Arc::new(RwLock::new(State::default())),
        }
    }

    pub fn with_default(storage: S) -> Self {
        Self::new(Default::default(), storage)
    }

    pub fn checkpoint(&self) -> u64 {
        self.state.read().unwrap().logical_position
    }

    pub fn load(settings: LsmSettings, storage: S) -> io::Result<Self> {
        let mut state = State::default();

        if storage.exists(FileId::IndexMap)? {
            let mut bytes = storage.read_all(FileId::IndexMap)?;

            state.logical_position = bytes.get_u64_le();

            // 17 stands for a level byte and an uuid encoded as a 128bits, which is 16bytes.
            while bytes.remaining() >= 17 {
                let level = bytes.get_u8();
                let id = Uuid::from_u128(bytes.get_u128_le());
                let table = SsTable::load(storage.clone(), id)?;

                state.levels.entry(level).or_default().push_back(table);
            }
        }

        Ok(Lsm {
            storage,
            buffer: BytesMut::default(),
            settings,
            state: Arc::new(RwLock::new(state)),
        })
    }

    pub fn rebuild(&self, manager: &ChunkManager<S>) -> io::Result<()> {
        let logical_position = self.state.read().unwrap().logical_position;
        let records = manager.prepare_logs(logical_position).map(|r| {
            let key = mikoshi_hash(&r.event_stream_id);

            (key, r.revision, r.logical_position)
        });

        self.put(records)?;

        let writer_checkpoint = manager.writer_checkpoint();
        let mut state = self.state.write().unwrap();
        state.logical_position = writer_checkpoint;

        Ok(())
    }

    pub fn ss_table_count(&self) -> usize {
        let state = self.state.read().unwrap();
        state.levels.values().map(|ts| ts.len()).sum()
    }

    pub fn ss_table_first(&self) -> Option<SsTable<S>> {
        let state = self.state.read().unwrap();
        let ts = state.levels.get(&0)?;
        ts.get(0).cloned()
    }

    pub fn put_values<V>(&self, values: V) -> io::Result<()>
    where
        V: IntoIterator<Item = (u64, u64, u64)>,
    {
        self.put(values.into_iter().lift())
    }

    pub fn put_single(&self, key: u64, revision: u64, position: u64) -> io::Result<()> {
        self.put_values(once((key, revision, position)))
    }

    pub fn put<Values>(&self, mut values: Values) -> io::Result<()>
    where
        Values: IteratorIO<Item = (u64, u64, u64)>,
    {
        let mut state = self.state.write().unwrap();
        let mut buffer = self.buffer.clone();

        while let Some((key, revision, position)) = values.next()? {
            state.active_table.put(key, revision, position);
            state.logical_position = position;
        }

        if state.active_table.size() < self.settings.mem_table_max_size {
            return Ok(());
        }

        let mem_table = std::mem::take(&mut state.active_table);
        let mut new_table = SsTable::new(self.storage.clone(), self.settings.base_block_size);

        new_table.put(&mut buffer, mem_table.entries().lift())?;

        let mut level = 0u8;
        let mut cleanups = Vec::new();

        loop {
            if let Some(tables) = state.levels.get_mut(&level) {
                if tables.len() + 1 >= self.settings.ss_table_max_count {
                    let mut targets = vec![new_table.iter()];
                    cleanups.push(new_table.id);

                    for table in tables.drain(..) {
                        targets.push(table.iter());
                        cleanups.push(table.id);
                    }

                    let values = Merge::new(targets).map(|e| (e.key, e.revision, e.position));

                    new_table = SsTable::new(self.storage.clone(), self.settings.base_block_size);
                    new_table.put(&mut buffer, values)?;

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

            state.levels.insert(level, tables);
            break;
        }

        // We only update the logical position this late because if we went beyond the main loop,
        // it means we actually flushed some data to disk. Anything prior is stored in mem-table.
        state.persist(&mut buffer, &self.storage)?;

        for id in cleanups {
            self.storage.remove(FileId::SSTable(id))?;
        }

        Ok(())
    }

    pub fn get(&self, key: u64, revision: u64) -> io::Result<Option<u64>> {
        let state = self.state.read().unwrap();
        let mut result = state.active_table.get(key, revision);

        if result.is_some() {
            return Ok(result);
        }

        for mem_table in state.immutable_tables.iter() {
            result = mem_table.get(key, revision);

            if result.is_some() {
                return Ok(result);
            }
        }

        for ss_tables in state.levels.values() {
            for table in ss_tables {
                result = table.find_key(key, revision)?.map(|e| e.position);

                if result.is_some() {
                    return Ok(result);
                }
            }
        }

        Ok(None)
    }

    pub fn scan(
        &self,
        key: u64,
        direction: Direction,
        start: Revision<u64>,
        count: usize,
    ) -> impl IteratorIO<Item = BlockEntry> + Send + Sync {
        let state = self.state.read().unwrap();
        let mut scans: Vec<Box<dyn IteratorIO<Item = BlockEntry> + Send + Sync>> = Vec::new();

        scans.push(Box::new(
            state.active_table.scan(key, direction, start, count).lift(),
        ));

        for mem_table in state.immutable_tables.iter() {
            scans.push(Box::new(
                mem_table.scan(key, direction, start, count).lift(),
            ));
        }

        for tables in state.levels.values() {
            for table in tables {
                scans.push(Box::new(table.scan(key, direction, start, count)));
            }
        }

        Merge::new(scans)
    }

    pub fn highest_revision(&self, key: u64) -> io::Result<Option<u64>> {
        Ok(self
            .scan(key, Direction::Backward, Revision::End, 1)
            .last()?
            .map(|e| e.revision))
    }
}
