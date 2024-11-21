use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::index::block::BLOCK_ENTRY_COUNT_SIZE;

use super::{
    get_block_size, Block, BlockEntry, BLOCK_ENTRY_SIZE, BLOCK_KEY_SIZE, BLOCK_OFFSET_SIZE,
};

pub struct BlockMut {
    data: BytesMut,
    capacity: usize,
    len: usize,
    offsets: Vec<u16>,
    first_key: Option<u64>,
    last_key: Option<u64>,
}

impl Default for BlockMut {
    fn default() -> Self {
        Self::new(4_096)
    }
}

impl BlockMut {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: BytesMut::new(),
            capacity,
            len: 0,
            offsets: vec![],
            first_key: None,
            last_key: None,
        }
    }

    pub fn try_from(mut data: BytesMut, bytes: Bytes) -> Option<Self> {
        let capacity = bytes.len();
        let len = bytes.slice(capacity - 2..).get_u16_le() as usize;
        let mut offset_section =
            bytes.slice((capacity - (len * BLOCK_OFFSET_SIZE + 2))..(capacity - 2));
        let mut offsets = Vec::with_capacity(offset_section.len() / BLOCK_OFFSET_SIZE);

        while offset_section.has_remaining() {
            let offset = offset_section.slice(..BLOCK_OFFSET_SIZE).get_u16_le();
            offsets.push(offset);
            offset_section.advance(BLOCK_OFFSET_SIZE);
        }

        data.put(bytes.slice(..len * BLOCK_ENTRY_SIZE));

        let mut first_key = None;
        let mut last_key = None;

        if !offsets.is_empty() {
            let first_entry = offsets.first().copied()? as usize;
            first_key = Some(
                bytes
                    .slice(first_entry..(first_entry + BLOCK_KEY_SIZE))
                    .get_u64_le(),
            );

            let last_entry = offsets.last().copied()? as usize;
            last_key = Some(
                bytes
                    .slice(last_entry..(last_entry + BLOCK_KEY_SIZE))
                    .get_u64_le(),
            );
        }

        Some(Self {
            data,
            capacity,
            len,
            offsets,
            first_key,
            last_key,
        })
    }

    pub fn try_add(&mut self, key: u64, revision: u64, position: u64) -> bool {
        if self.estimated_size() + BLOCK_ENTRY_SIZE + BLOCK_OFFSET_SIZE > self.capacity {
            return false;
        }

        let offset = self.data.len() as u16;
        self.data.put_u64_le(key);
        self.data.put_u64_le(revision);
        self.data.put_u64_le(position);
        self.offsets.push(offset);

        self.len += 1;

        true
    }

    pub fn try_read(&self, index: usize) -> Option<BlockEntry> {
        if index > self.len() - 1 {
            return None;
        }

        let offset = self.offsets[index] as usize;
        let mut entry_section = &self.data.as_ref()[offset..(offset + BLOCK_ENTRY_SIZE)];

        let key = entry_section.get_u64_le();
        let revision = entry_section.get_u64_le();
        let position = entry_section.get_u64_le();

        Some(BlockEntry {
            key,
            revision,
            position,
        })
    }

    pub fn estimated_size(&self) -> usize {
        get_block_size(self.len)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn scan_forward(&self, key: u64, start: u64, max: usize) -> ScanForward<'_> {
        ScanForward {
            inner: &self.data,
            key,
            start,
            count: 0,
            max,
        }
    }
    pub fn scan_backward(&self, key: u64, start: u64, max: usize) -> ScanBackward<'_> {
        ScanBackward {
            inner: &self.data,
            key,
            start,
            count: 0,
            max,
        }
    }

    pub fn freeze(mut self) -> Block {
        let entries_end = self.len() * BLOCK_ENTRY_SIZE;
        let offset_section_start =
            self.capacity - (self.len() * BLOCK_OFFSET_SIZE + BLOCK_ENTRY_COUNT_SIZE);

        self.data.put_bytes(0, offset_section_start - entries_end);

        for offset in self.offsets {
            self.data.put_u16_le(offset);
        }

        self.data.put_u16_le(self.len as u16);

        Block::new(self.data.freeze())
    }
}

pub struct ScanForward<'a> {
    inner: &'a [u8],
    key: u64,
    start: u64,
    count: usize,
    max: usize,
}

impl<'a> Iterator for ScanForward<'a> {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.count >= self.max {
                return None;
            }

            if self.inner.is_empty() {
                return None;
            }

            let current_key = self.inner.get_u64_le();

            if self.key < current_key {
                self.count = self.max;
                return None;
            }

            if self.key > current_key {
                self.inner.advance(std::mem::size_of::<u64>() * 2);
                continue;
            }

            let revision = self.inner.get_u64_le();

            if self.start > revision {
                self.inner.advance(std::mem::size_of::<u64>());
                continue;
            }

            self.count += 1;

            return Some(BlockEntry {
                key: self.key,
                revision,
                position: self.inner.get_u64_le(),
            });
        }
    }
}

pub struct ScanBackward<'a> {
    inner: &'a [u8],
    key: u64,
    start: u64,
    count: usize,
    max: usize,
}

impl<'a> Iterator for ScanBackward<'a> {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.count >= self.max {
                return None;
            }

            if self.inner.is_empty() {
                return None;
            }

            let mut section = &self.inner[(self.inner.len() - BLOCK_ENTRY_SIZE)..];
            let current_key = section.get_u64_le();

            if self.key > current_key {
                self.count = self.max;
                return None;
            }

            if self.key < current_key {
                self.inner = &self.inner[(self.inner.len() - BLOCK_ENTRY_SIZE)..];
                continue;
            }

            let revision = section.get_u64_le();

            if self.start > revision {
                self.inner = &self.inner[(self.inner.len() - BLOCK_ENTRY_SIZE)..];
                continue;
            }

            self.count += 1;
            self.inner = &self.inner[(self.inner.len() - BLOCK_ENTRY_SIZE)..];

            return Some(BlockEntry {
                key: self.key,
                revision,
                position: section.get_u64_le(),
            });
        }
    }
}
