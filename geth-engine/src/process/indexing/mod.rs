mod client;
mod proc;

use super::RunnableRaw;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use geth_common::{Direction, IteratorIO};
use geth_mikoshi::storage::Storage;

pub use client::{IndexClient, Streaming};
pub use proc::Indexing;

const ENTRY_SIZE: usize = 2 * std::mem::size_of::<u64>();

pub struct StoreRequestBuilder {
    inner: BytesMut,
}

impl StoreRequestBuilder {
    pub fn new(mut inner: BytesMut, key: u64) -> Self {
        inner.put_u8(0x01);
        inner.put_u64_le(key);

        Self { inner }
    }

    pub fn put_entry(&mut self, revision: u64, position: u64) {
        self.inner.put_u64_le(revision);
        self.inner.put_u64_le(position);
    }

    pub fn build(self) -> Bytes {
        self.inner.freeze()
    }
}

enum IndexingReq {
    Read {
        key: u64,
        start: u64,
        count: u64,
        dir: Direction,
    },

    Store {
        key: u64,
        entries: Bytes,
    },

    LatestRevision {
        key: u64,
    },
}

impl IndexingReq {
    fn try_from(mut bytes: Bytes) -> Option<Self> {
        match bytes.get_u8() {
            0x00 => Some(Self::Read {
                key: bytes.get_u64_le(),
                start: bytes.get_u64_le(),
                count: bytes.get_u64_le(),
                dir: match bytes.get_u8() {
                    0x00 => Direction::Forward,
                    0x01 => Direction::Backward,
                    _ => unreachable!(),
                },
            }),

            0x01 => Some(Self::Store {
                key: bytes.get_u64_le(),
                entries: bytes,
            }),

            0x02 => Some(Self::LatestRevision {
                key: bytes.get_u64_le(),
            }),

            _ => unreachable!(),
        }
    }

    pub fn read(mut buffer: BytesMut, key: u64, start: u64, count: usize, dir: Direction) -> Bytes {
        buffer.put_u8(0x00);
        buffer.put_u64_le(key);
        buffer.put_u64_le(start);
        buffer.put_u64_le(count as u64);
        buffer.put_u8(match dir {
            Direction::Forward => 0,
            Direction::Backward => 1,
        });

        buffer.freeze()
    }

    pub fn store(buffer: BytesMut, key: u64) -> StoreRequestBuilder {
        StoreRequestBuilder::new(buffer, key)
    }

    pub fn latest_revision(mut buffer: BytesMut, key: u64) -> Bytes {
        buffer.put_u8(0x02);
        buffer.put_u64_le(key);
        buffer.freeze()
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum IndexingResp {
    StreamDeleted,
    Committed,
    Streaming,
    Error,
}

impl IndexingResp {
    fn serialize(self, mut buffer: BytesMut) -> Bytes {
        match self {
            IndexingResp::StreamDeleted => buffer.put_u8(0x00),
            IndexingResp::Committed => buffer.put_u8(0x01),
            IndexingResp::Streaming => buffer.put_u8(0x02),
            IndexingResp::Error => buffer.put_u8(0x03),
        }

        buffer.split().freeze()
    }

    fn try_from(mut bytes: Bytes) -> eyre::Result<Self> {
        match bytes.get_u8() {
            0x00 => Ok(IndexingResp::StreamDeleted),
            0x01 => Ok(IndexingResp::Committed),
            0x02 => Ok(IndexingResp::Streaming),
            0x03 => Ok(IndexingResp::Error),
            _ => eyre::bail!("unknown response message from from the index process"),
        }
    }

    fn expect(self, expectation: IndexingResp) -> eyre::Result<()> {
        if self != expectation {
            eyre::bail!("expected {:?} but got {:?}", expectation, self);
        }

        Ok(())
    }
}
