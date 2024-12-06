use std::io;
use std::{cmp::Ordering, collections::VecDeque};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use uuid::Uuid;

use geth_common::{IteratorIO, IteratorIOExt};
use geth_mikoshi::storage::{FileId, Storage};

use crate::index::block::{Block, BlockEntry};

use super::block::get_block_size;
use super::block::mutable::BlockMut;

const SSTABLE_META_ENTRY_SIZE: usize =
    std::mem::size_of::<u32>() + std::mem::size_of::<u64>() + std::mem::size_of::<u64>();

const SSTABLE_HEADER_SIZE: usize = std::mem::size_of::<u32>();

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
// TODO - Using bytes impedes peformance in our case because we can do many random accesses that waste time decoding integers.
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
    pub buffer: BytesMut,
}

impl<S> SsTable<S>
where
    S: Storage,
{
    pub fn new(storage: S, block_size: usize) -> Self {
        Self::with_buffer(storage, block_size, BytesMut::new())
    }

    pub fn with_capacity(storage: S, num_elems: usize) -> Self {
        Self::new(storage, get_block_size(num_elems))
    }

    pub fn with_buffer(storage: S, block_size: usize, buffer: BytesMut) -> Self {
        debug_assert!(
            block_size >= get_block_size(1),
            "block_size doesn't have the minimum viable value: {}",
            get_block_size(1)
        );

        Self {
            id: Uuid::new_v4(),
            storage,
            metas: BlockMetas(Default::default()),
            meta_offset: 0,
            block_size,
            buffer,
        }
    }

    pub fn with_default(storage: S) -> Self {
        SsTable::new(storage, 4_096)
    }

    pub fn load_with_buffer(storage: S, raw_id: Uuid, buffer: BytesMut) -> io::Result<Self> {
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
            buffer,
        })
    }

    pub fn load(storage: S, raw_id: Uuid) -> io::Result<Self> {
        Self::load_with_buffer(storage, raw_id, BytesMut::new())
    }

    pub fn file_id(&self) -> FileId {
        FileId::SSTable(self.id)
    }

    pub fn find_best_candidates(&self, key: u64, revision: u64) -> VecDeque<usize> {
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

                Ordering::Equal => return VecDeque::from([mid as usize]),
            }
        }

        if closest_lowest == closest_highest {
            return VecDeque::from([closest_lowest]);
        }

        VecDeque::from([closest_lowest, closest_highest])
    }

    pub fn len(&self) -> usize {
        self.metas.len()
    }

    pub fn read_block(&self, block_idx: usize) -> io::Result<Block> {
        let meta = self.metas.read(block_idx);
        let block_bytes =
            self.storage
                .read_from(self.file_id(), meta.offset as u64, self.block_size)?;

        Ok(Block::from(self.block_size, block_bytes))
    }

    pub fn find_key(&self, key: u64, revision: u64) -> io::Result<Option<BlockEntry>> {
        for block_idx in self.find_best_candidates(key, revision) {
            let block = self.read_block(block_idx)?;

            if let Some(entry) = block.find(key, revision) {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }

    pub fn put_iter<Values>(&mut self, values: Values) -> io::Result<()>
    where
        Values: IntoIterator<Item = (u64, u64, u64)>,
    {
        self.put(values.into_iter().lift())
    }

    pub fn put<Values>(&mut self, mut values: Values) -> io::Result<()>
    where
        Values: IteratorIO<Item = (u64, u64, u64)>,
    {
        let mut builder = BlockMut::new(self.buffer.split(), self.block_size);
        let mut metas = self.buffer.split();
        let mut block_start_offset = std::mem::size_of::<u32>();

        self.buffer.put_u32_le(self.block_size as u32);

        self.storage
            .write_to(self.file_id(), 0, self.buffer.split().freeze())?;

        while let Some((key, rev, pos)) = values.next()? {
            let mut retried = false;

            loop {
                if builder.try_add(key, rev, pos) {
                    if builder.len() == 1 {
                        metas.put_u32_le(block_start_offset as u32);
                        metas.put_u64_le(key);
                        metas.put_u64_le(rev);
                    }

                    break;
                }

                if retried {
                    // This should be impossible since we have contingencies when creating a new ss table.
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        format!(
                            "invalid block_size of {} lead to not be able to perform any write",
                            self.block_size
                        ),
                    ));
                }

                self.storage
                    .append(self.file_id(), builder.split_then_build())?;

                block_start_offset = self.storage.offset(self.file_id())? as usize;
                retried = true;
            }
        }

        if !builder.is_empty() {
            self.storage
                .append(self.file_id(), builder.split_then_build())?;
        }

        let meta_offset = self.storage.offset(self.file_id())?;
        let metas = metas.freeze();

        self.storage.append(self.file_id(), metas.clone())?;
        self.metas = BlockMetas::new(metas);
        self.meta_offset = meta_offset as u64;

        self.buffer.put_u32_le(meta_offset as u32);

        self.storage
            .append(self.file_id(), self.buffer.split().freeze())?;

        Ok(())
    }

    pub fn iter(&self) -> SsTableIter<S> {
        SsTableIter {
            block_idx: 0,
            entry_idx: 0,
            block: None,
            table: self.clone(),
        }
    }

    pub fn iter_tuples(&self) -> impl IteratorIO<Item = (u64, u64, u64)> {
        self.iter().map(|t| (t.key, t.revision, t.position))
    }

    pub fn scan_forward(&self, key: u64, start: u64, count: usize) -> ScanForward<S> {
        ScanForward {
            key,
            revision: start,
            count,
            block_idx: 0,
            block_scan: None,
            table: self,
            candidates: self.find_best_candidates(key, start),
        }
    }

    pub fn scan_backward(&self, key: u64, start: u64, count: usize) -> ScanBackward<S> {
        let mut candidates = self.find_best_candidates(key, start);

        candidates.rotate_left(candidates.len() - 1);

        ScanBackward {
            key,
            revision: start,
            count,
            block_idx: None,
            block_scan: None,
            table: self,
            candidates,
        }
    }
}

pub struct SsTableIter<S> {
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
                if let Some(entry) = block.try_read(self.entry_idx) {
                    self.entry_idx += 1;

                    return Ok(Some(entry));
                }

                self.block = None;
                self.entry_idx = 0;
                self.block_idx += 1;
            }

            return Ok(None);
        }
    }
}

pub struct ScanForward<'a, S> {
    key: u64,
    revision: u64,
    count: usize,
    block_idx: usize,
    block_scan: Option<crate::index::block::immutable::ScanForward>,
    table: &'a SsTable<S>,
    candidates: VecDeque<usize>,
}

impl<'a, S> IteratorIO for ScanForward<'a, S>
where
    S: Storage,
{
    type Item = BlockEntry;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        loop {
            if self.count == 0 {
                return Ok(None);
            }

            if self.block_scan.is_none() && self.block_idx >= self.table.len() {
                return Ok(None);
            }

            if self.block_scan.is_none() {
                if let Some(block_idx) = self.candidates.pop_front() {
                    self.block_idx = block_idx;
                }

                let block = self.table.read_block(self.block_idx)?;

                // There is no need to continue loading blocks because from that point, no further block will contains the key we are looking for.
                if !block.contains(self.key) {
                    self.count = 0;
                    return Ok(None);
                }

                self.block_scan = Some(block.scan_forward(self.key, self.revision, self.count));
                self.block_idx += 1;
            }

            if let Some(entry) = self.block_scan.as_mut().unwrap().next() {
                self.count = self.count.checked_sub(1).unwrap_or_default();
                return Ok(Some(entry));
            }

            self.block_scan = None;
        }
    }
}

pub struct ScanBackward<'a, S> {
    key: u64,
    revision: u64,
    count: usize,
    block_idx: Option<usize>,
    block_scan: Option<crate::index::block::immutable::ScanBackward>,
    table: &'a SsTable<S>,
    candidates: VecDeque<usize>,
}

impl<'a, S> IteratorIO for ScanBackward<'a, S>
where
    S: Storage,
{
    type Item = BlockEntry;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        loop {
            if self.count == 0 {
                return Ok(None);
            }

            if let Some(mut iter) = self.block_scan.take() {
                if let Some(entry) = iter.next() {
                    self.count = self.count.checked_sub(1).unwrap_or_default();
                    self.block_scan = Some(iter);
                    return Ok(Some(entry));
                }
            }

            let block_idx = if let Some(block_idx) = self.candidates.pop_front() {
                block_idx
            } else if let Some(prev_block_id) = self.block_idx {
                if let Some(new_block_idx) = prev_block_id.checked_sub(1) {
                    new_block_idx
                } else {
                    self.count = 0;
                    return Ok(None);
                }
            } else {
                self.count = 0;
                return Ok(None);
            };

            let block = self.table.read_block(block_idx)?;
            let last_key = block.last_key.unwrap_or_default();

            if !block.contains(last_key) {
                continue;
            }

            self.block_scan = Some(block.scan_backward(self.key, self.revision, self.count));
        }
    }
}
pub enum NoSSTable {}

impl IteratorIO for NoSSTable {
    type Item = BlockEntry;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        unreachable!()
    }
}
