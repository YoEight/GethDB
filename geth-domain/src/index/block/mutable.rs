use bytes::{BufMut, Bytes, BytesMut};

use crate::index::block::BLOCK_ENTRY_COUNT_SIZE;

use super::{get_block_size, BLOCK_ENTRY_SIZE, BLOCK_OFFSET_SIZE};

pub struct WrittenEntry {
    pub start_offset: usize,
    pub len: usize,
}

pub struct BlockMut {
    data: BytesMut,
    capacity: usize,
    len: usize,
    offsets: Vec<u16>,
    first_key: Option<u64>,
    last_key: Option<u64>,
}

impl BlockMut {
    pub fn new(buffer: BytesMut, capacity: usize) -> Self {
        Self {
            data: buffer,
            capacity,
            len: 0,
            offsets: vec![],
            first_key: None,
            last_key: None,
        }
    }

    pub fn try_add(&mut self, key: u64, revision: u64, position: u64) -> Option<WrittenEntry> {
        if self.estimated_size() + BLOCK_ENTRY_SIZE + BLOCK_OFFSET_SIZE > self.capacity {
            return None;
        }

        let offset = self.data.len();
        self.data.put_u64_le(key);
        self.data.put_u64_le(revision);
        self.data.put_u64_le(position);
        self.offsets.push(offset as u16);

        self.len += 1;

        Some(WrittenEntry {
            start_offset: offset,
            len: BLOCK_ENTRY_SIZE,
        })
    }

    pub fn estimated_size(&self) -> usize {
        get_block_size(self.len)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn split_then_build(&mut self) -> Bytes {
        let mut data = self.data.split();
        let entries_end = self.len() * BLOCK_ENTRY_SIZE;
        let offset_section_start =
            self.capacity - (self.len() * BLOCK_OFFSET_SIZE + BLOCK_ENTRY_COUNT_SIZE);

        data.put_bytes(0, offset_section_start - entries_end);

        for offset in &self.offsets {
            data.put_u16_le(*offset);
        }

        data.put_u16_le(self.len as u16);

        self.len = 0;
        self.offsets.clear();
        self.first_key = None;
        self.last_key = None;

        data.freeze()
    }
}
