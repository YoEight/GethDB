use crate::index::rannoch::block::{Block, BlockEntry, Scan, BLOCK_ENTRY_SIZE};
use crate::index::rannoch::range_start;
use crate::index::{IteratorIO, IteratorIOExt};
use crate::storage::{FileId, Storage};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cmp::Ordering;
use std::io;
use std::mem::take;
use std::net::ToSocketAddrs;
use std::ops::RangeBounds;
use uuid::Uuid;

const SSTABLE_META_ENTRY_SIZE: usize = 4 + 8 + 8;
const SSTABLE_HEADER_SIZE: usize = 4;

#[derive(Debug, Clone, Copy)]
pub struct BlockMeta {
    pub offset: u32,
    pub key: u64,
    pub revision: u64,
}

impl BlockMeta {
    pub fn compare_key_id(&self, key: u64, revision: u64) -> Ordering {
        let key_ord = self.key.cmp(&key);

        if key_ord.is_ne() {
            return key_ord;
        }

        self.revision.cmp(&revision)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockMetas(Bytes);

impl BlockMetas {
    pub fn new(bytes: Bytes) -> Self {
        Self(bytes)
    }

    pub fn read(&self, idx: usize) -> BlockMeta {
        let offset = idx * SSTABLE_META_ENTRY_SIZE;
        let mut bytes = self.0.clone();

        bytes.advance(offset);
        let mut bytes = bytes.copy_to_bytes(SSTABLE_META_ENTRY_SIZE);

        BlockMeta {
            offset: bytes.get_u32_le(),
            key: bytes.get_u64_le(),
            revision: bytes.get_u64_le(),
        }
    }

    pub fn len(&self) -> usize {
        self.0.len() / SSTABLE_META_ENTRY_SIZE
    }

    pub fn last_block_first_key_offset(&self) -> Option<usize> {
        if self.len() == 0 {
            return None;
        }

        Some(self.read(self.len() - 1).offset as usize)
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct SsTable<S> {
    pub id: Uuid,
    pub storage: S,
    pub metas: BlockMetas,
    pub meta_offset: u64,
    pub block_size: usize,
}

impl<S> SsTable<S>
where
    S: Storage,
{
    pub fn new(storage: S, block_size: usize) -> Self {
        Self {
            id: Uuid::new_v4(),
            storage,
            metas: BlockMetas(Default::default()),
            meta_offset: 0,
            block_size,
        }
    }

    pub fn with_default(storage: S) -> Self {
        SsTable::new(storage, 4_096)
    }

    pub fn load(storage: S, raw_id: Uuid) -> io::Result<Self> {
        let id = FileId::SSTable(raw_id);
        let len = storage.len(id)?;
        let block_size = storage.read_from(id, 0, SSTABLE_HEADER_SIZE)?.get_u32_le() as usize;
        let meta_offset = storage.read_from(id, len as u64 - 4, 4)?.get_u32_le() as u64;
        let metas = storage.read_from(id, meta_offset, len - 4usize - meta_offset as usize)?;

        Ok(SsTable {
            id: raw_id,
            storage,
            metas: BlockMetas::new(metas),
            meta_offset,
            block_size,
        })
    }

    pub fn file_type(&self) -> FileId {
        FileId::SSTable(self.id)
    }

    pub fn find_best_candidates(&self, key: u64, revision: u64) -> Vec<usize> {
        let mut closest_lowest = 0usize;
        let mut closest_highest = 0usize;
        let mut low = 0i64;
        let mut high = (self.len() - 1) as i64;

        while low <= high {
            let mid = (low + high) / 2;
            let meta = self.metas.read(mid as usize);

            match meta.compare_key_id(key, revision) {
                Ordering::Less => {
                    closest_lowest = mid as usize;
                    low = mid + 1;
                }

                Ordering::Greater => {
                    closest_highest = mid as usize;
                    high = mid - 1;
                }

                Ordering::Equal => return vec![mid as usize],
            }
        }

        if closest_lowest == closest_highest {
            return vec![closest_lowest];
        }

        vec![closest_lowest, closest_highest]
    }

    pub fn len(&self) -> usize {
        self.metas.len()
    }

    pub fn read_block(&self, block_idx: usize) -> io::Result<Block> {
        let meta = self.metas.read(block_idx);
        let block_size = self.block_actual_size(block_idx);
        let block_bytes =
            self.storage
                .read_from(self.file_type(), meta.offset as u64, block_size)?;

        Ok(Block::new(block_bytes))
    }

    pub fn find_key(&self, key: u64, revision: u64) -> io::Result<Option<BlockEntry>> {
        for block_idx in self.find_best_candidates(key, revision) {
            let block = self.read_block(block_idx)?;

            if let Some(entry) = block.find_entry(key, revision) {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }

    pub fn put_iter<Values>(&mut self, buffer: &mut BytesMut, mut values: Values) -> io::Result<()>
    where
        Values: IntoIterator<Item = (u64, u64, u64)>,
    {
        self.put(buffer, values.into_iter().lift())
    }

    pub fn put<Values>(&mut self, buffer: &mut BytesMut, mut values: Values) -> io::Result<()>
    where
        Values: IteratorIO<Item = (u64, u64, u64)>,
    {
        let mut block_current_size = 0usize;
        let mut metas = Vec::new();

        buffer.put_u32_le(self.block_size as u32);
        while let Some((key, rev, pos)) = values.next()? {
            if block_current_size + BLOCK_ENTRY_SIZE > self.block_size {
                let remaining = self.block_size - block_current_size;

                buffer.put_bytes(0, remaining);
                block_current_size = 0;
            }

            let offset = buffer.len() as u32;
            buffer.put_u64_le(key);
            buffer.put_u64_le(rev);
            buffer.put_u64_le(pos);

            if block_current_size == 0 {
                metas.put_u32_le(offset);
                metas.put_u64_le(key);
                metas.put_u64_le(rev);
            }

            block_current_size += BLOCK_ENTRY_SIZE;
        }

        let meta_offset = buffer.len() as u32;
        let metas = Bytes::from(metas);
        buffer.put(metas.clone());
        buffer.put_u32_le(meta_offset);

        self.storage
            .write_to(self.file_type(), 0, buffer.split().freeze())?;
        self.metas = BlockMetas::new(metas);
        self.meta_offset = meta_offset as u64;

        Ok(())
    }

    pub fn block_actual_size(&self, block_idx: usize) -> usize {
        if block_idx + 1 >= self.len() {
            self.meta_offset as usize - self.metas.read(block_idx).offset as usize
        } else {
            self.block_size
        }
    }

    pub fn iter(&self) -> SsTableIter<S> {
        SsTableIter {
            block_size: self.block_size,
            block_idx: 0,
            entry_idx: 0,
            block: None,
            table: self.clone(),
        }
    }

    pub fn iter_tuples(&self) -> impl IteratorIO<Item = (u64, u64, u64)> {
        self.iter().map(|t| (t.key, t.revision, t.position))
    }

    pub fn scan<R>(&self, key: u64, range: R) -> SsTableScan<S, R>
    where
        R: RangeBounds<u64> + Clone,
    {
        let candidates = self.find_best_candidates(key, range_start(range.clone()));
        SsTableScan {
            range,
            key,
            block_idx: 0,
            block_size: self.block_size,
            block: None,
            table: self.clone(),
            candidates,
        }
    }
}

pub struct SsTableIter<S> {
    block_size: usize,
    block_idx: usize,
    entry_idx: usize,
    block: Option<Block>,
    table: SsTable<S>,
}

impl<S> IteratorIO for SsTableIter<S>
where
    S: Storage,
{
    type Item = BlockEntry;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        loop {
            if self.block_idx >= self.table.len() {
                return Ok(None);
            }

            if self.block.is_none() {
                self.block = Some(self.table.read_block(self.block_idx)?);
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

pub struct SsTableScan<S, R> {
    range: R,
    key: u64,
    block_idx: usize,
    block_size: usize,
    block: Option<Scan<R>>,
    table: SsTable<S>,
    candidates: Vec<usize>,
}

impl<S, R> IteratorIO for SsTableScan<S, R>
where
    R: RangeBounds<u64> + Clone,
    S: Storage,
{
    type Item = BlockEntry;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        loop {
            if self.block_idx >= self.table.len() {
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
                let block = self.table.read_block(self.block_idx)?;

                self.block = Some(block.scan(self.key, self.range.clone()));

                continue;
            }

            if self.block_idx <= self.table.len() - 1 {
                self.candidates.push(self.block_idx);
                continue;
            }

            return Ok(None);
        }
    }
}
