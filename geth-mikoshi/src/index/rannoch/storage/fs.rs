use crate::index::rannoch::block::{Block, BlockEntry, Scan, BLOCK_ENTRY_SIZE};
use crate::index::rannoch::mem_table::MEM_TABLE_ENTRY_SIZE;
use crate::index::rannoch::range_start;
use crate::index::rannoch::ss_table::{BlockMetas, SsTable};
use crate::index::IteratorIO;
use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
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

    pub fn sst_load(&self, id: Uuid) -> io::Result<Option<SsTable>> {
        if let Some(file) = self.inner.get(&id) {
            let len = file.metadata()?.len();
            let mut buffer = self.buffer.clone();

            buffer.resize(4, 0);
            file.read_exact_at(&mut buffer, len - 4)?;
            let meta_offset = buffer.get_u32_le() as u64;

            buffer.resize((len - 4 - meta_offset) as usize, 0);
            file.read_exact_at(&mut buffer, meta_offset)?;

            return Ok(Some(SsTable {
                id,
                metas: BlockMetas::new(buffer.freeze()),
            }));
        }

        Ok(None)
    }

    pub fn sst_put<Values>(&mut self, table: &mut SsTable, mut values: Values) -> io::Result<()>
    where
        Values: IntoIterator<Item = (u64, u64, u64)>,
    {
        let file = if let Some(file) = self.inner.get(&table.id) {
            file.clone()
        } else {
            let filepath = self.root.join(table.id.to_string());
            let file = OpenOptions::new().read(true).write(true).open(filepath)?;
            let file = Arc::new(file);

            self.inner.insert(table.id, file.clone());
            file
        };

        let mut block_current_size = 0usize;
        let mut metas = self.buffer.clone();
        for (key, rev, pos) in values {
            if block_current_size + BLOCK_ENTRY_SIZE > self.block_size {
                let remaining = self.block_size - block_current_size;

                self.buffer.put_bytes(0, remaining);
                block_current_size = 0;
            }

            self.buffer.put_u64_le(key);
            self.buffer.put_u64_le(rev);
            self.buffer.put_u64_le(pos);

            if block_current_size == 0 {
                metas.put_u32_le(self.buffer.len() as u32);
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
        table: &SsTable,
        file: Option<Arc<File>>,
        block_size: usize,
        key: u64,
        range: R,
    ) -> Self {
        let candidates = table.find_best_candidates(key, range_start(range.clone()));

        Self {
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
    type Item = ();

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        todo!()
    }
}
