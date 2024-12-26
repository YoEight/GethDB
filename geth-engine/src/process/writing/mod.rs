mod client;
mod proc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use geth_common::ExpectedRevision;

pub use client::WriterClient;
pub use proc::run;

enum Request {
    Append {
        ident: Bytes,
        expected: ExpectedRevision,
        index: bool,
        events: Bytes,
    },
}

impl Request {
    fn try_from(mut bytes: Bytes) -> Option<Self> {
        let len = bytes.get_u16_le() as usize;
        let ident = bytes.copy_to_bytes(len);
        let expected = deserialize_expected_revision(&mut bytes)?;
        let index = bytes.get_u8() != 0x00;

        Some(Self::Append {
            ident,
            expected,
            index,
            events: bytes,
        })
    }

    pub fn append_builder<'a>(
        buffer: &'a mut BytesMut,
        stream: &str,
        expected: ExpectedRevision,
        index: bool,
    ) -> AppendBuilder<'a> {
        AppendBuilder::new(buffer, stream, expected, index)
    }
}

pub struct AppendBuilder<'a> {
    pub buffer: &'a mut BytesMut,
}

impl<'a> AppendBuilder<'a> {
    pub fn new(
        buffer: &'a mut BytesMut,
        stream: &str,
        expected: ExpectedRevision,
        index: bool,
    ) -> Self {
        let ident = stream.as_bytes();
        buffer.put_u16_le(ident.len() as u16);
        buffer.extend_from_slice(ident);
        serialize_expected_revision(buffer, expected);
        buffer.put_u8(if index { 0x01 } else { 0x00 });

        Self { buffer }
    }

    pub fn push(&mut self, payload: &[u8]) {
        self.buffer.put_u32_le(payload.len() as u32);
        self.buffer.extend_from_slice(payload);
    }

    pub fn build(self) -> Bytes {
        self.buffer.split().freeze()
    }
}

fn serialize_expected_revision(bytes: &mut BytesMut, expected: ExpectedRevision) {
    match expected {
        ExpectedRevision::Any => bytes.put_u8(0x00),
        ExpectedRevision::NoStream => bytes.put_u8(0x01),
        ExpectedRevision::StreamExists => bytes.put_u8(0x02),
        ExpectedRevision::Revision(r) => {
            bytes.put_u8(0x03);
            bytes.put_u64_le(r);
        }
    }
}

fn deserialize_expected_revision(bytes: &mut Bytes) -> Option<ExpectedRevision> {
    match bytes.get_u8() {
        0x00 => Some(ExpectedRevision::Any),
        0x01 => Some(ExpectedRevision::NoStream),
        0x02 => Some(ExpectedRevision::StreamExists),
        0x03 => {
            let revision = bytes.get_u64_le();
            Some(ExpectedRevision::Revision(revision))
        }

        _ => {
            tracing::warn!("unknown expected revision flag");
            None
        }
    }
}

enum Response {
    Error,

    Deleted,

    WrongExpectedRevision {
        expected: ExpectedRevision,
        current: ExpectedRevision,
    },

    Committed {
        start: u64,
        next: u64,
    },
}

impl Response {
    fn wrong_expected_revision(expected: ExpectedRevision, current: ExpectedRevision) -> Self {
        Self::WrongExpectedRevision { expected, current }
    }

    fn error() -> Self {
        Self::Error
    }

    fn committed(start: u64, next: u64) -> Self {
        Self::Committed { start, next }
    }

    fn serialize(self, bytes: &mut BytesMut) -> Bytes {
        match self {
            Response::Error => bytes.put_u8(0x00),
            Response::Deleted => bytes.put_u8(0x01),
            Response::WrongExpectedRevision { expected, current } => {
                bytes.put_u8(0x02);
                serialize_expected_revision(bytes, expected);
                serialize_expected_revision(bytes, current);
            }
            Response::Committed { start, next } => {
                bytes.put_u8(0x03);
                bytes.put_u64_le(start);
                bytes.put_u64_le(next);
            }
        }

        bytes.split().freeze()
    }

    fn try_from(mut bytes: Bytes) -> Option<Self> {
        match bytes.get_u8() {
            0x00 => Some(Response::Error),

            0x01 => Some(Response::Deleted),

            0x02 => {
                let expected = deserialize_expected_revision(&mut bytes)?;
                let current = deserialize_expected_revision(&mut bytes)?;

                Some(Self::WrongExpectedRevision { expected, current })
            }

            0x03 => Some(Response::Committed {
                start: bytes.get_u64_le(),
                next: bytes.get_u64_le(),
            }),

            _ => None,
        }
    }
}
