use crate::index::rannoch::block::{Block, BlockEntry, Scan, BLOCK_ENTRY_SIZE};
use crate::index::rannoch::lsm::{sst_table_block_count_limit, Lsm, LsmSettings};
use crate::index::rannoch::mem_table::MEM_TABLE_ENTRY_SIZE;
use crate::index::rannoch::range_start;
use crate::index::rannoch::ss_table::{BlockMetas, SsTable};
use crate::index::{IteratorIO, IteratorIOExt, MergeIO};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::ErrorKind;
use std::ops::RangeBounds;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct FsStorage {
    root: PathBuf,
    block_size: usize,
    buffer: BytesMut,
    inner: HashMap<Uuid, Arc<File>>,
}

impl FsStorage {
    pub fn new(root: PathBuf, block_size: usize) -> Self {
        Self {
            root,
            block_size,
            buffer: Default::default(),
            inner: Default::default(),
        }
    }

    pub fn new_with_default(root: PathBuf) -> Self {
        Self::new(root, 4_096)
    }

    pub fn sst_read_block(&self, table: &SsTable, block_idx: usize) -> io::Result<Option<Block>> {
        if let Some(file) = self.inner.get(&table.id) {
            return sst_read_block(self.buffer.clone(), file, table, self.block_size, block_idx);
        }

        Ok(None)
    }

    pub fn sst_find_key(
        &self,
        table: &SsTable,
        key: u64,
        revision: u64,
    ) -> io::Result<Option<BlockEntry>> {
        for block_idx in table.find_best_candidates(key, revision) {
            let block = self.sst_read_block(table, block_idx)?;

            if let Some(block) = block {
                if let Some(entry) = block.find_entry(key, revision) {
                    return Ok(Some(entry));
                }
            }
        }

        Ok(None)
    }

    pub fn sst_load(&mut self, id: Uuid) -> io::Result<Option<SsTable>> {
        if let Some(file) = self.inner.get(&id) {
            return Ok(Some(sst_load(&mut self.buffer, &file, id)?));
        }

        Ok(None)
    }

    pub fn sst_put<Values>(&mut self, table: &mut SsTable, mut values: Values) -> io::Result<()>
    where
        Values: IteratorIO<Item = (u64, u64, u64)>,
    {
        let file = if let Some(file) = self.inner.get(&table.id) {
            file.clone()
        } else {
            let filepath = self.root.join(table.id.to_string());
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(filepath)?;

            let file = Arc::new(file);

            self.inner.insert(table.id, file.clone());
            file
        };

        let mut block_current_size = 0usize;
        let mut metas = self.buffer.clone();
        while let Some((key, rev, pos)) = values.next()? {
            if block_current_size + BLOCK_ENTRY_SIZE > self.block_size {
                let remaining = self.block_size - block_current_size;

                self.buffer.put_bytes(0, remaining);
                block_current_size = 0;
            }

            let offset = self.buffer.len() as u32;
            self.buffer.put_u64_le(key);
            self.buffer.put_u64_le(rev);
            self.buffer.put_u64_le(pos);

            if block_current_size == 0 {
                metas.put_u32_le(offset);
                metas.put_u64_le(key);
                metas.put_u64_le(rev);
            }

            block_current_size += BLOCK_ENTRY_SIZE;
        }

        let meta_offset = self.buffer.len() as u32;
        let metas = metas.freeze();
        self.buffer.put(metas.clone());
        self.buffer.put_u32_le(meta_offset);
        table.metas = BlockMetas::new(metas);

        let bytes = self.buffer.split().freeze();
        file.write_all_at(&bytes, 0)?;
        file.sync_all()?;

        Ok(())
    }

    pub fn sst_iter(&self, table: &SsTable) -> SsTableIter {
        let file = self.inner.get(&table.id).cloned();
        SsTableIter {
            buffer: self.buffer.clone(),
            block_size: self.block_size,
            block_idx: 0,
            entry_idx: 0,
            block: None,
            file,
            table: table.clone(),
        }
    }

    pub fn sst_iter_tuples(&self, table: &SsTable) -> impl IteratorIO<Item = (u64, u64, u64)> {
        self.sst_iter(table)
            .map(|t| (t.key, t.revision, t.position))
    }

    pub fn sst_scan<R>(&self, table: &SsTable, key: u64, range: R) -> SsTableScan<R>
    where
        R: RangeBounds<u64> + Clone,
    {
        SsTableScan::new(
            self.buffer.clone(),
            table,
            self.inner.get(&table.id).cloned(),
            self.block_size,
            key,
            range,
        )
    }

    pub fn lsm_put(
        &mut self,
        lsm: &mut Lsm,
        key: u64,
        revision: u64,
        position: u64,
    ) -> io::Result<()> {
        lsm.active_table.put(key, revision, position);

        if lsm.active_table.size() < lsm.settings.mem_table_max_size {
            return Ok(());
        }

        let mem_table = std::mem::take(&mut lsm.active_table);
        let mut new_table = SsTable::new();

        self.sst_put(&mut new_table, mem_table.entries().lift())?;
        let mut level = 0u8;
        let mut cleanups = Vec::new();

        loop {
            if let Some(tables) = lsm.levels.get_mut(&level) {
                if tables.len() + 1 >= lsm.settings.ss_table_max_count {
                    let mut targets = vec![self.sst_iter(&new_table)];
                    self.inner.remove(&new_table.id);
                    cleanups.push(new_table.id);

                    for table in tables.drain(..) {
                        targets.push(self.sst_iter(&table));
                        self.inner.remove(&table.id);
                        cleanups.push(table.id);
                    }

                    let values = MergeIO::new(targets).map(|e| (e.key, e.revision, e.position));

                    new_table = SsTable::new();
                    self.sst_put(&mut new_table, values)?;

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

        // We only update the logical position this late because if we went beyond the main loop,
        // it means we actually flushed some data to disk. Anything prior is stored in mem-table.
        lsm.logical_position = position;

        self.write_index_file(lsm)?;

        for id in cleanups {
            std::fs::remove_file(self.root.join(id.to_string()))?;
        }

        Ok(())
    }

    pub fn lsm_get(&self, lsm: &Lsm, key: u64, revision: u64) -> io::Result<Option<u64>> {
        let mut result = lsm.active_table.get(key, revision);

        if result.is_some() {
            return Ok(result);
        }

        for mem_table in lsm.immutable_tables.iter() {
            result = mem_table.get(key, revision);

            if result.is_some() {
                return Ok(result);
            }
        }

        for ss_tables in lsm.levels.values() {
            for table in ss_tables {
                result = self.sst_find_key(table, key, revision)?.map(|e| e.position);

                if result.is_some() {
                    return Ok(result);
                }
            }
        }

        Ok(None)
    }

    pub fn lsm_scan<R>(&self, lsm: &Lsm, key: u64, range: R) -> impl IteratorIO<Item = BlockEntry>
    where
        R: RangeBounds<u64> + Clone + 'static,
    {
        let mut scans: Vec<Box<dyn IteratorIO<Item = BlockEntry>>> = Vec::new();

        scans.push(Box::new(lsm.active_table.scan(key, range.clone()).lift()));

        for mem_table in lsm.immutable_tables.iter() {
            scans.push(Box::new(mem_table.scan(key, range.clone()).lift()));
        }

        for tables in lsm.levels.values() {
            for table in tables {
                scans.push(Box::new(self.sst_scan(table, key, range.clone())));
            }
        }

        MergeIO::new(scans)
    }

    #[cfg(test)]
    pub fn test_lsm_serialize(&mut self, lsm: &Lsm) -> io::Result<()> {
        self.write_index_file(lsm)
    }

    fn write_index_file(&mut self, lsm: &Lsm) -> io::Result<()> {
        self.buffer.put_u64_le(lsm.logical_position);
        self.buffer.put_u32_le(self.block_size as u32);

        for (level, tables) in &lsm.levels {
            for table in tables {
                self.buffer.put_u8(*level);
                self.buffer.put_u128_le(table.id.as_u128());
            }
        }

        let bytes = self.buffer.split().freeze();
        let filepath = self.root.join("indexmap");
        let file = OpenOptions::new().write(true).create(true).open(filepath)?;

        file.write_all_at(&bytes, 0)?;
        file.sync_all()?;

        Ok(())
    }

    pub fn lsm_load(&mut self, settings: LsmSettings) -> io::Result<Option<Lsm>> {
        let filepath = self.root.join("indexmap");
        let mut bytes = match std::fs::read(filepath) {
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
            Ok(bytes) => Bytes::from(bytes),
        };

        let logical_position = bytes.get_u64_le();
        let block_size = bytes.get_u32_le();
        let mut levels = BTreeMap::<u8, VecDeque<SsTable>>::new();

        // 17 stands for a level byte and an uuid encoded as a 128bits, which is 16bytes.
        while bytes.remaining() >= 17 {
            let level = bytes.get_u8();
            let id = Uuid::from_u128(bytes.get_u128_le());
            let filepath = self.root.join(id.to_string());
            let file = OpenOptions::new().read(true).open(filepath)?;
            let table = sst_load(&mut self.buffer, &file, id)?;

            levels.entry(level).or_default().push_back(table);
        }

        Ok(Some(Lsm {
            settings,
            active_table: Default::default(),
            immutable_tables: Default::default(),
            logical_position,
            levels,
        }))
    }
}

/// TODO - This function is quite suitable for caching. For example, it possible a block might
/// frequently be asked and we could just old its value instead of reading from disk.
fn sst_read_block(
    mut buffer: BytesMut,
    file: &Arc<File>,
    table: &SsTable,
    block_size: usize,
    block_idx: usize,
) -> io::Result<Option<Block>> {
    if block_idx >= table.len() {
        return Ok(None);
    }

    let block_meta = table.metas.read(block_idx);
    let len = file.metadata()?.len();

    buffer.resize(4, 0);
    file.read_exact_at(&mut buffer[..], len - 4)?;

    let meta_offset = buffer.get_u32_le() as usize;
    let block_actual_size = if block_idx + 1 >= table.len() {
        meta_offset - block_meta.offset as usize
    } else {
        block_size
    };

    buffer.resize(block_actual_size, 0);
    file.read_exact_at(&mut buffer[..], block_meta.offset as u64)?;

    Ok(Some(Block::new(buffer.freeze())))
}

fn sst_load(buffer: &mut BytesMut, file: &File, id: Uuid) -> io::Result<SsTable> {
    let len = file.metadata()?.len();

    buffer.resize(4, 0);
    file.read_exact_at(buffer, len - 4)?;
    let meta_offset = buffer.get_u32_le() as u64;

    buffer.resize((len - 4 - meta_offset) as usize, 0);
    file.read_exact_at(buffer, meta_offset)?;

    return Ok(SsTable {
        id,
        metas: BlockMetas::new(buffer.split().freeze()),
    });
}

pub struct SsTableIter {
    buffer: BytesMut,
    block_size: usize,
    block_idx: usize,
    entry_idx: usize,
    block: Option<Block>,
    file: Option<Arc<File>>,
    table: SsTable,
}

impl IteratorIO for SsTableIter {
    type Item = BlockEntry;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        loop {
            if self.file.is_none() || self.block_idx >= self.table.len() {
                return Ok(None);
            }

            if self.block.is_none() {
                let file = self.file.as_ref().unwrap();

                self.block = sst_read_block(
                    self.buffer.split(),
                    file,
                    &self.table,
                    self.block_size,
                    self.block_idx,
                )?;
            }

            if let Some(block) = self.block.as_ref() {
                if self.entry_idx >= block.len() {
                    self.block = None;
                    self.entry_idx = 0;
                    self.block_idx += 1;

                    continue;
                }

                if let Some(entry) = block.read_entry(self.entry_idx) {
                    self.entry_idx += 1;

                    return Ok(Some(entry));
                }
            }

            return Ok(None);
        }
    }
}

pub struct SsTableScan<R> {
    buffer: BytesMut,
    range: R,
    key: u64,
    file: Option<Arc<File>>,
    block_idx: usize,
    block_size: usize,
    block: Option<Scan<R>>,
    table: SsTable,
    candidates: Vec<usize>,
}

impl<R> SsTableScan<R>
where
    R: RangeBounds<u64> + Clone,
{
    pub fn new(
        buffer: BytesMut,
        table: &SsTable,
        file: Option<Arc<File>>,
        block_size: usize,
        key: u64,
        range: R,
    ) -> Self {
        let candidates = table.find_best_candidates(key, range_start(range.clone()));

        Self {
            buffer,
            range,
            key,
            file,
            block_idx: 0,
            block_size,
            block: None,
            table: table.clone(),
            candidates,
        }
    }
}

impl<R> IteratorIO for SsTableScan<R>
where
    R: RangeBounds<u64> + Clone,
{
    type Item = BlockEntry;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        loop {
            if self.file.is_none() {
                return Ok(None);
            }

            if let Some(mut block) = self.block.take() {
                if let Some(entry) = block.next() {
                    self.block = Some(block);
                    return Ok(Some(entry));
                }

                self.block_idx += 1;
                continue;
            }

            if !self.candidates.is_empty() {
                self.block_idx = self.candidates.remove(0);
                let block = sst_read_block(
                    self.buffer.clone(),
                    self.file.as_ref().unwrap(),
                    &self.table,
                    self.block_size,
                    self.block_idx,
                )?;

                if let Some(block) = block {
                    self.block = Some(block.scan(self.key, self.range.clone()));
                    continue;
                }

                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Corrupted SStable '{:?}', block {} does not exist",
                        self.file.as_ref().unwrap(),
                        self.block_idx
                    ),
                ));
            }

            if self.block_idx <= self.table.len() - 1 {
                self.candidates.push(self.block_idx);
                continue;
            }

            return Ok(None);
        }
    }
}
