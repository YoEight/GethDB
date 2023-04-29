use crate::index::rannoch::block::{Block, BlockEntry, Scan, BLOCK_ENTRY_SIZE};
use crate::index::rannoch::lsm::{sst_table_block_count_limit, Lsm, LSM_BASE_SSTABLE_BLOCK_COUNT};
use crate::index::rannoch::merge::Merge;
use crate::index::rannoch::ss_table::{BlockMetas, SsTable};
use crate::index::rannoch::{range_start, IndexedPosition};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use nom::character::complete::tab;
use std::collections::{HashMap, VecDeque};
use std::ops::RangeBounds;
use uuid::Uuid;

#[derive(Clone)]
pub struct InMemStorage {
    block_size: usize,
    buffer: BytesMut,
    inner: HashMap<Uuid, Bytes>,
}

impl Default for InMemStorage {
    fn default() -> Self {
        InMemStorage::new(4_096)
    }
}

impl InMemStorage {
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            buffer: Default::default(),
            inner: Default::default(),
        }
    }

    pub fn sst_read_block(&self, table: &SsTable, block_idx: usize) -> Option<Block> {
        if block_idx >= table.len() {
            return None;
        }

        let bytes = self.inner.get(&table.id)?;
        sst_read_block(bytes, table, self.block_size, block_idx)
    }

    pub fn sst_find_key(&self, table: &SsTable, key: u64, revision: u64) -> Option<BlockEntry> {
        for block_idx in table.find_best_candidates(key, revision) {
            let block = self.sst_read_block(table, block_idx)?;

            if let Some(entry) = block.find_entry(key, revision) {
                return Some(entry);
            }
        }

        None
    }

    pub fn sst_load(&self, id: Uuid) -> Option<SsTable> {
        let mut bytes = self.inner.get(&id)?.clone();
        let mut footer = &bytes[bytes.len() - 4..];
        let meta_offset = footer.get_u32_le() as usize;

        bytes.advance(meta_offset);
        let metas = bytes.copy_to_bytes(bytes.len() - 4);

        Some(SsTable {
            id,
            metas: BlockMetas::new(metas),
        })
    }

    pub fn sst_put<Values>(&mut self, table: &mut SsTable, mut values: Values)
    where
        Values: IntoIterator<Item = (u64, u64, u64)>,
    {
        let mut table_bytes = self.inner.get(&table.id).cloned().unwrap_or_default();
        let mut offset = 0usize;
        let mut block_current_size = 0usize;

        if !table_bytes.is_empty() {
            let mut footer = &table_bytes[table_bytes.len() - 4..];
            let meta_offset = footer.get_u32_le() as usize;

            offset = meta_offset;
            block_current_size = meta_offset - table.metas.last_block_first_key_offset().unwrap();

            self.buffer.put(table_bytes.copy_to_bytes(meta_offset));
        }

        let mut meta = BytesMut::from(table.metas.as_slice());
        for (key, revision, position) in values {
            if block_current_size + BLOCK_ENTRY_SIZE > self.block_size {
                let remaining = self.block_size - block_current_size;

                self.buffer.put_bytes(0, remaining);
                offset += remaining;
                block_current_size = 0;
            }

            self.buffer.put_u64_le(key);
            self.buffer.put_u64_le(revision);
            self.buffer.put_u64_le(position);

            if block_current_size == 0 {
                meta.put_u32_le(offset as u32);
                meta.put_u64_le(key);
                meta.put_u64_le(revision);
            }

            block_current_size += BLOCK_ENTRY_SIZE;
            offset += BLOCK_ENTRY_SIZE;
        }

        let new_meta = meta.freeze();

        self.buffer.put(new_meta.clone());
        self.buffer.put_u32_le(offset as u32);
        self.inner.insert(table.id, self.buffer.split().freeze());
        table.metas = BlockMetas::new(new_meta);
    }

    pub fn sst_iter(&self, table: &SsTable) -> SsTableIter {
        let block_bytes = if let Some(mut bytes) = self.inner.get(&table.id).cloned() {
            let mut footer = &bytes[bytes.len() - 4..];
            let meta_offset = footer.get_u32_le() as usize;

            bytes.copy_to_bytes(meta_offset)
        } else {
            Default::default()
        };

        SsTableIter {
            block_size: self.block_size,
            block_idx: 0,
            entry_idx: 0,
            block: None,
            block_bytes,
            table: table.clone(),
        }
    }

    pub fn sst_scan<R>(&self, table: &SsTable, key: u64, range: R) -> SsTableScan<R>
    where
        R: RangeBounds<u64> + Clone,
    {
        SsTableScan::new(
            table,
            self.inner.get(&table.id).cloned().unwrap_or_default(),
            self.block_size,
            key,
            range,
        )
    }

    pub fn sst_iter_tuples(&self, table: &SsTable) -> impl Iterator<Item = (u64, u64, u64)> {
        self.sst_iter(table)
            .map(|t| (t.key, t.revision, t.position))
    }

    pub fn lsm_put(&mut self, lsm: &mut Lsm, key: u64, revision: u64, position: u64) {
        lsm.active_table.put(key, revision, position);

        if lsm.active_table.size() < lsm.settings.mem_table_max_size {
            return;
        }

        let mem_table = std::mem::take(&mut lsm.active_table);
        let mut new_table = SsTable::new();

        self.sst_put(&mut new_table, mem_table.entries());
        let mut level = 0u8;

        loop {
            if let Some(tables) = lsm.levels.get_mut(&level) {
                if tables.len() + 1 >= lsm.settings.ss_table_max_count {
                    let mut targets = vec![self.sst_iter(&new_table)];
                    self.inner.remove(&new_table.id);

                    for table in tables.drain(..) {
                        targets.push(self.sst_iter(&table));
                        self.inner.remove(&table.id);
                    }

                    let values = Merge::new(targets).map(|e| (e.key, e.revision, e.position));

                    new_table = SsTable::new();
                    self.sst_put(&mut new_table, values);

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

            lsm.levels.insert(level, tables);
            break;
        }
    }

    pub fn lsm_get(&self, lsm: &Lsm, key: u64, revision: u64) -> Option<u64> {
        let mut result = lsm.active_table.get(key, revision);

        if result.is_some() {
            return result;
        }

        for mem_table in lsm.immutable_tables.iter() {
            result = mem_table.get(key, revision);

            if result.is_some() {
                return result;
            }
        }

        for ss_tables in lsm.levels.values() {
            for table in ss_tables {
                result = self.sst_find_key(table, key, revision).map(|e| e.position);

                if result.is_some() {
                    return result;
                }
            }
        }

        None
    }

    pub fn lsm_scan<R>(&self, lsm: &Lsm, key: u64, range: R) -> impl Iterator<Item = BlockEntry>
    where
        R: RangeBounds<u64> + Clone + 'static,
    {
        let mut scans: Vec<Box<dyn Iterator<Item = BlockEntry>>> = Vec::new();

        scans.push(Box::new(lsm.active_table.scan(key, range.clone())));

        for mem_table in lsm.immutable_tables.iter() {
            scans.push(Box::new(mem_table.scan(key, range.clone())));
        }

        for tables in lsm.levels.values() {
            for table in tables {
                scans.push(Box::new(self.sst_scan(table, key, range.clone())));
            }
        }

        Merge::new(scans)
    }
}

pub struct SsTableIter {
    block_size: usize,
    block_idx: usize,
    entry_idx: usize,
    block: Option<Block>,
    block_bytes: Bytes,
    table: SsTable,
}

impl Iterator for SsTableIter {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.block_bytes.is_empty() && self.block.is_none() {
                return None;
            }

            if self.block_idx >= self.table.len() {
                return None;
            }

            if self.block.is_none() {
                let bytes = if self.block_idx == self.table.len() - 1 {
                    self.block_bytes.copy_to_bytes(self.block_bytes.len())
                } else {
                    self.block_bytes.copy_to_bytes(self.block_size)
                };

                self.block = Some(Block::new(bytes));
            }

            let block = self.block.as_ref()?;

            if self.entry_idx >= block.len() {
                self.block = None;
                self.entry_idx = 0;
                self.block_idx += 1;

                continue;
            }

            let entry = block.read_entry(self.entry_idx)?;
            self.entry_idx += 1;

            return Some(entry);
        }
    }
}

pub fn sst_read_block(
    bytes: &Bytes,
    table: &SsTable,
    block_size: usize,
    block_idx: usize,
) -> Option<Block> {
    if block_idx >= table.len() {
        return None;
    }

    let block_meta = table.metas.read(block_idx);
    let mut bytes = bytes.clone();
    let mut meta_offset_bytes = &bytes[bytes.len() - 4..];
    let meta_offset = meta_offset_bytes.get_u32_le() as usize;
    let len = bytes.len();

    bytes.advance(block_meta.offset as usize);

    let size = if block_idx + 1 >= table.len() {
        meta_offset - block_meta.offset as usize
    } else {
        block_size
    };

    Some(Block::new(bytes.copy_to_bytes(size)))
}

pub struct SsTableScan<R> {
    range: R,
    key: u64,
    bytes: Bytes,
    block_idx: usize,
    block: Option<Scan<R>>,
    table: SsTable,
    candidates: Vec<usize>,
    block_size: usize,
}

impl<R> SsTableScan<R>
where
    R: RangeBounds<u64> + Clone,
{
    pub fn new(table: &SsTable, bytes: Bytes, block_size: usize, key: u64, range: R) -> Self {
        let candidates = table.find_best_candidates(key, range_start(range.clone()));

        Self {
            range,
            key,
            bytes,
            block_idx: 0,
            block: None,
            table: table.clone(),
            block_size,
            candidates,
        }
    }
}

impl<R> Iterator for SsTableScan<R>
where
    R: RangeBounds<u64> + Clone,
{
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO - Track if a block scan immediately returns nothing. It basically means there is no
        // point at trying further block as other blocks have 0 chances of hosting the values we
        // are looking for.
        loop {
            if let Some(mut block) = self.block.take() {
                if let Some(entry) = block.next() {
                    self.block = Some(block);
                    return Some(entry);
                }

                self.block_idx += 1;
                continue;
            }

            if !self.candidates.is_empty() {
                self.block_idx = self.candidates.remove(0);
                let block =
                    sst_read_block(&self.bytes, &self.table, self.block_size, self.block_idx)?;

                self.block = Some(block.scan(self.key, self.range.clone()));
                continue;
            }

            if self.block_idx <= self.table.len() - 1 {
                self.candidates.push(self.block_idx);
                continue;
            }

            return None;
        }
    }
}
