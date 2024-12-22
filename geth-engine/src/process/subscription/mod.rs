use bytes::{Buf, BufMut, Bytes, BytesMut};
use geth_common::Revision;

mod client;
mod proc;
pub enum Request {
    Subscribe { ident: Bytes },
    Push { events: Bytes },
    Unsubscribe { ident: Bytes },
}

impl Request {
    fn try_from(mut bytes: Bytes) -> Option<Self> {
        let ident_len = bytes.get_u16_le() as usize;
        let ident = bytes.copy_to_bytes(ident_len);

        Some(Self::Subscribe { ident })
    }

    fn subscribe(buffer: &mut BytesMut, stream_name: &str, start: Revision<u64>) -> Bytes {
        let ident = stream_name.as_bytes();
        buffer.put_u16_le(ident.len() as u16);
        buffer.extend_from_slice(ident);

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
