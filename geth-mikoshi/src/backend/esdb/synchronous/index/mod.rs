use bytes::Buf;
use geth_common::Position;
use uuid::Uuid;

mod entry;
mod map;
mod mem_table;

pub const MD5_SIZE: usize = 16;
pub const PTABLE_HEADER_SIZE: usize = 128;
pub const PTABLE_FOOTER_SIZE: usize = 128;
pub const ENTRY_SIZE: usize = 24;
pub const KEY_SIZE: usize = 16;

#[derive(Copy, Clone, Debug)]
pub struct Key {
    pub stream: u64,
    pub revision: u64,
}

#[derive(Copy, Clone, Debug)]
pub struct Midpoint {
    key: Key,
    offset: u64,
}

#[derive(Copy, Clone, Debug)]
pub struct PTableHeader {
    pub version: u8,
}

impl PTableHeader {
    pub fn load<B>(mut src: B) -> eyre::Result<Self>
    where
        B: Buf,
    {
        if src.get_u8() != 1u8 {
            eyre::bail!("Invalid PTable file");
        }

        Ok(Self {
            version: src.get_u8(),
        })
    }
}

#[derive(Copy, Clone, Debug)]
pub struct PTableFooter {
    pub version: u8,
    pub cached_midpoints_num: usize,
}

impl PTableFooter {
    pub fn load<B>(mut src: B) -> eyre::Result<Self>
    where
        B: Buf,
    {
        if src.get_u8() != 1u8 {
            eyre::bail!("Invalid PTable file");
        }

        let version = src.get_u8();

        let cached_midpoints_num = src.get_u32_le() as usize;

        Ok(Self {
            version,
            cached_midpoints_num,
        })
    }

    pub fn cached_midpoint_size(&self) -> usize {
        self.cached_midpoints_num * ENTRY_SIZE
    }
}

#[derive(Debug)]
pub struct PTable {
    pub id: Uuid,
    pub level: u64,
    pub order: i32,
    pub header: PTableHeader,
    pub footer: PTableFooter,
    pub midpoints: Vec<Midpoint>,
}

#[derive(Debug)]
pub struct IndexMap {
    pub version: u64,
    pub position: Position,
    pub auto_merge_level: u64,
    pub tables: Vec<PTable>,
}
