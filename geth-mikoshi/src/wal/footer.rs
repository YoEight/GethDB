use bitflags::bitflags;
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub const CHUNK_FOOTER_SIZE: usize = 128;

bitflags! {
    pub struct PrepareFlags: u16 {
        const NO_DATA = 0x00;
        const HAS_DATA = 0x01;
        const TRANSACTION_START = 0x02;
        const TRANSACTION_END = 0x04;
        const DELETED_STREAM = 0x08;
        const IS_COMMITTED = 0x20;
        const IS_JSON = 0x100;
    }
}

bitflags! {
    pub struct FooterFlags: u8 {
        const IS_COMPLETED = 0x1;
        const IS_MAP_12_BYTES = 0x2;
    }
}

#[derive(Debug, Clone)]
pub struct ChunkFooter {
    pub flags: FooterFlags,
    pub physical_data_size: usize,
    pub logical_data_size: usize,
    pub hash: Bytes,
}

impl ChunkFooter {
    pub fn get(mut buf: Bytes) -> Option<Self> {
        let flags = FooterFlags::from_bits(buf.get_u8()).expect("valid footer flags");
        let is_completed = flags.contains(FooterFlags::IS_COMPLETED);

        if !is_completed {
            return None;
        }

        let physical_data_size = buf.get_u32_le() as usize;
        let logical_data_size = buf.get_u64_le() as usize;

        buf.advance(buf.remaining() - 16);

        Some(ChunkFooter {
            flags,
            physical_data_size,
            logical_data_size,
            hash: buf,
        })
    }

    pub fn put(&self, buf: &mut BytesMut) {
        let len = buf.len();
        buf.put_u8(self.flags.bits);
        buf.put_u32_le(self.physical_data_size as u32);
        buf.put_u64_le(self.logical_data_size as u64);

        let written = buf.len() - len;
        let free_space_size = CHUNK_FOOTER_SIZE - written - 16;

        // Unused space.
        buf.put_bytes(0, free_space_size);
        buf.put(self.hash.clone());
    }
}