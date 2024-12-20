use bytes::{Buf, BufMut, Bytes, BytesMut};
use geth_common::{Direction, Position, Record, Revision};
use geth_mikoshi::wal::LogEntry;
use uuid::Uuid;

mod client;
mod proc;

pub use client::ReaderClient;
pub use client::Streaming;
pub use proc::Reading;

pub enum Request {
    Read {
        ident: Bytes,
        start: Revision<u64>,
        direction: Direction,
        count: usize,
    },
}

impl Request {
    fn try_from(mut bytes: Bytes) -> Option<Self> {
        let ident_len = bytes.get_u16_le() as usize;
        let ident = bytes.copy_to_bytes(ident_len);

        let start = match bytes.get_u8() {
            0x00 => Revision::Start,
            0x01 => Revision::End,
            0x02 => Revision::Revision(bytes.get_u64_le()),
            _ => return None,
        };

        let direction = match bytes.get_u8() {
            0x00 => Direction::Forward,
            0x01 => Direction::Backward,
            _ => return None,
        };

        let count = bytes.get_u64_le() as usize;

        Some(Self::Read {
            ident,
            start,
            direction,
            count,
        })
    }

    fn read(
        buffer: &mut BytesMut,
        stream_name: &str,
        start: Revision<u64>,
        direction: Direction,
        count: usize,
    ) -> Bytes {
        let ident = stream_name.as_bytes();
        buffer.put_u16_le(ident.len() as u16);
        buffer.extend_from_slice(ident);

        match start {
            Revision::Start => buffer.put_u8(0x00),
            Revision::End => buffer.put_u8(0x01),
            Revision::Revision(r) => {
                buffer.put_u8(0x02);
                buffer.put_u64_le(r);
            }
        }

        match direction {
            Direction::Forward => buffer.put_u8(0x00),
            Direction::Backward => buffer.put_u8(0x01),
        }

        buffer.put_u64_le(count as u64);

        buffer.split().freeze()
    }
}

pub enum Response {
    Error,
    StreamDeleted,
    Streaming,
}

impl Response {
    fn serialize(self, buffer: &mut BytesMut) -> Bytes {
        match self {
            Response::Error => buffer.put_u8(0x00),
            Response::StreamDeleted => buffer.put_u8(0x01),
            Response::Streaming => buffer.put_u8(0x02),
        }

        buffer.split().freeze()
    }

    fn try_from(mut bytes: Bytes) -> Option<Self> {
        if bytes.len() != 1 {
            return None;
        }

        match bytes.get_u8() {
            0x00 => Some(Response::Error),
            0x01 => Some(Response::StreamDeleted),
            0x02 => Some(Response::Streaming),
            _ => None,
        }
    }
}

trait LogEntryExt {
    fn into_record(self) -> Record;
    fn serialize(self, buffer: &mut BytesMut);

    fn deserialize(bytes: &mut Bytes) -> Self;
}

impl LogEntryExt for LogEntry {
    fn into_record(mut self) -> Record {
        let revision = self.payload.get_u64_le();
        let str_len = self.payload.get_u16_le() as usize;
        let stream_name =
            unsafe { String::from_utf8_unchecked(self.payload.copy_to_bytes(str_len).to_vec()) };

        let id = Uuid::from_u128_le(self.payload.get_u128_le());
        let str_len = self.payload.get_u16_le() as usize;
        let r#type =
            unsafe { String::from_utf8_unchecked(self.payload.copy_to_bytes(str_len).to_vec()) };

        Record {
            id,
            r#type,
            stream_name,
            position: Position(self.position),
            revision,
            data: self.payload,
        }
    }

    fn serialize(self, buffer: &mut BytesMut) {
        buffer.put_u64_le(self.position);
        buffer.put_u8(self.r#type);
        buffer.put_u32_le(self.payload.len() as u32);
        buffer.extend_from_slice(&self.payload);
    }

    fn deserialize(bytes: &mut Bytes) -> Self {
        let position = bytes.get_u64_le();
        let r#type = bytes.get_u8();
        let len = bytes.get_u32_le() as usize;
        Self {
            position,
            r#type,
            payload: bytes.copy_to_bytes(len),
        }
    }
}
