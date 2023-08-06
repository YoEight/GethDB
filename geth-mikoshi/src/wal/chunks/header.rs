use crate::constants::CHUNK_HEADER_SIZE;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub struct ChunkHeader {
    pub version: u8,
    pub chunk_size: usize,
    pub chunk_start_number: usize,
    pub chunk_end_number: usize,
    pub chunk_id: Uuid,
}

impl ChunkHeader {
    pub fn put(&self, buf: &mut BytesMut) {
        buf.put_u8(self.version);
        buf.put_u32_le(self.chunk_size as u32);
        buf.put_u32_le(self.chunk_start_number as u32);
        buf.put_u32_le(self.chunk_end_number as u32);
        buf.put_u128_le(self.chunk_id.as_u128());
        buf.put_bytes(0, CHUNK_HEADER_SIZE - buf.len());
    }

    pub fn get(mut buf: Bytes) -> Self {
        let version = buf.get_u8();
        let chunk_size = buf.get_u32_le() as usize;
        let chunk_start_number = buf.get_u32_le() as usize;
        let chunk_end_number = buf.get_u32_le() as usize;
        let chunk_id = Uuid::from_u128(buf.get_u128_le());

        Self {
            version,
            chunk_size,
            chunk_start_number,
            chunk_end_number,
            chunk_id,
        }
    }
}
