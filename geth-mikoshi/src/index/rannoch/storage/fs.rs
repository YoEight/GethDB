use crate::index::rannoch::block::{Block, BlockEntry};
use crate::index::rannoch::ss_table::SsTable;
use bytes::{Buf, BytesMut};
use std::collections::HashMap;
use std::fs::File;
use std::io;
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

            if let Some(entry) = block.find_entry(key, revision) {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }
}

/// TODO - This function is quite suitable for caching. For example, it possible a block might
/// frequently asked and we could just old its value instead of reading from disk.
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
