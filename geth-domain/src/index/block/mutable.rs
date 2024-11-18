use bytes::{Buf, BufMut, Bytes, BytesMut};

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
                    .slice(first_entry..(first_entry + BLOCK_KEY_SIZE))
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

    pub fn freeze(self) -> Block {
        let offset_section_start = self.capacity - (self.len() * BLOCK_OFFSET_SIZE + 2);
        todo!()
    }
}
