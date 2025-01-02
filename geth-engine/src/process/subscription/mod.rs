use bytes::{Buf, BufMut, Bytes, BytesMut};

mod client;
mod proc;

pub use client::SubscriptionClient;
pub use proc::run;

pub enum Request {
    Subscribe { ident: Bytes },
    Push { events: Bytes },
    // Unsubscribe { ident: Bytes },
}

impl Request {
    fn try_from(mut bytes: Bytes) -> Option<Self> {
        match bytes.get_u8() {
            0x00 => {
                let ident_len = bytes.get_u16_le() as usize;
                let ident = bytes.copy_to_bytes(ident_len);
                Some(Self::Subscribe { ident })
            }

            0x01 => Some(Self::Push { events: bytes }),

            _ => None,
        }
    }

    fn subscribe(buffer: &mut BytesMut, stream_name: &str) -> Bytes {
        buffer.put_u8(0x00);
        let ident = stream_name.as_bytes();
        buffer.put_u16_le(ident.len() as u16);
        buffer.extend_from_slice(ident);

        buffer.split().freeze()
    }

    pub fn push(buffer: &mut BytesMut) -> PushBuilder<'_> {
        PushBuilder::new(buffer)
    }
}

pub struct PushBuilder<'a> {
    buffer: &'a mut BytesMut,
}

impl<'a> PushBuilder<'a> {
    pub fn new(buffer: &'a mut BytesMut) -> Self {
        buffer.put_u8(0x01);
        Self { buffer }
    }

    pub fn push_entry(&mut self, bytes: Bytes) {
        self.buffer.extend_from_slice(&bytes);
    }

    pub fn build(self) -> Bytes {
        self.buffer.split().freeze()
    }
}

pub enum Response {
    Error,
    Confirmed,
}

impl Response {
    fn serialize(self, buffer: &mut BytesMut) -> Bytes {
        match self {
            Response::Error => buffer.put_u8(0x00),
            Response::Confirmed => buffer.put_u8(0x01),
        }

        buffer.split().freeze()
    }

    fn try_from(mut bytes: Bytes) -> Option<Self> {
        if bytes.len() != 1 {
            return None;
        }

        match bytes.get_u8() {
            0x00 => Some(Response::Error),
            0x01 => Some(Response::Confirmed),
            _ => None,
        }
    }
}
