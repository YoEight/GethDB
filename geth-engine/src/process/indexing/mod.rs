mod client;
mod proc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use geth_common::{Direction, IteratorIO};
use geth_mikoshi::storage::Storage;

pub use client::{IndexClient, Streaming};
pub use proc::run;

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

enum Request {
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

impl Request {
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
pub enum Response {
    StreamDeleted,
    Committed,
    Streaming,
    Error,
}

impl Response {
    fn serialize(self, mut buffer: BytesMut) -> Bytes {
        match self {
            Response::StreamDeleted => buffer.put_u8(0x00),
            Response::Committed => buffer.put_u8(0x01),
            Response::Streaming => buffer.put_u8(0x02),
            Response::Error => buffer.put_u8(0x03),
        }

        buffer.split().freeze()
    }

    fn try_from(mut bytes: Bytes) -> eyre::Result<Self> {
        match bytes.get_u8() {
            0x00 => Ok(Response::StreamDeleted),
            0x01 => Ok(Response::Committed),
            0x02 => Ok(Response::Streaming),
            0x03 => Ok(Response::Error),
            _ => eyre::bail!("protocol error when dealing with the index process"),
        }
    }

    fn expect(self, expectation: Response) -> eyre::Result<()> {
        if self != expectation {
            eyre::bail!("expected {:?} but got {:?}", expectation, self);
        }

        Ok(())
    }
}
