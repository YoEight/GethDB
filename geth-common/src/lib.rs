use std::string::{FromUtf16Error, FromUtf8Error};

use byteorder::{BigEndian, ByteOrder};
use bytes::Bytes;
use uuid::Uuid;

mod google {
    pub mod rpc {
        include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
    }
}

mod geth {
    pub mod protocol {
        include!(concat!(env!("OUT_DIR"), "/event_store.client.rs"));
        pub mod streams {
            include!(concat!(env!("OUT_DIR"), "/event_store.client.streams.rs"));
        }
    }
}

pub mod protocol {
    pub use super::geth::protocol::{Empty, StreamIdentifier, Uuid};

    pub mod uuid {
        pub use super::super::geth::protocol::uuid::*;
    }

    pub mod streams {
        pub use super::super::geth::protocol::streams::read_req::options::{
            stream_options::RevisionOption, CountOption, StreamOption,
        };
        pub use super::super::geth::protocol::streams::*;
        pub use super::super::geth::protocol::streams::{
            append_req,
            append_resp::{self, success::CurrentRevisionOption, Success},
            read_resp::{
                self,
                read_event::{self, RecordedEvent},
                ReadEvent,
            },
        };
        pub mod server {
            pub use super::super::super::geth::protocol::streams::streams_server::*;
        }
    }
}

impl From<Uuid> for protocol::Uuid {
    fn from(value: Uuid) -> Self {
        uuid_to_grpc(value)
    }
}

impl TryFrom<protocol::Uuid> for Uuid {
    type Error = uuid::Error;

    fn try_from(value: protocol::Uuid) -> Result<Self, Self::Error> {
        grpc_to_uuid(value)
    }
}

fn uuid_to_grpc(value: Uuid) -> protocol::Uuid {
    let buf = value.as_bytes();

    let most_significant_bits = BigEndian::read_i64(&buf[0..8]);
    let least_significant_bits = BigEndian::read_i64(&buf[8..16]);

    protocol::Uuid {
        value: Some(protocol::uuid::Value::Structured(
            protocol::uuid::Structured {
                most_significant_bits,
                least_significant_bits,
            },
        )),
    }
}

fn grpc_to_uuid(value: protocol::Uuid) -> Result<Uuid, uuid::Error> {
    let value = if let Some(value) = value.value {
        value
    } else {
        return Ok(Uuid::nil());
    };

    match value {
        protocol::uuid::Value::Structured(s) => {
            let mut buf = [0u8; 16];

            BigEndian::write_i64(&mut buf, s.most_significant_bits);
            BigEndian::write_i64(&mut buf[8..16], s.least_significant_bits);

            Ok(Uuid::from_bytes(buf))
        }

        protocol::uuid::Value::String(s) => Uuid::try_from(s.as_str()),
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Revision<A> {
    Start,
    End,
    Revision(A),
}

#[derive(Clone, Copy, Debug)]
pub enum Direction {
    Forward,
    Backward,
}

impl TryFrom<i32> for Direction {
    type Error = WrongDirectionError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Direction::Forward),
            1 => Ok(Direction::Backward),
            _ => Err(WrongDirectionError),
        }
    }
}

pub struct WrongDirectionError;

#[derive(Debug)]
pub struct Propose {
    pub id: Uuid,
    pub data: Bytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ExpectedRevision {
    Revision(u64),
    NoStream,
    Any,
    StreamsExists,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Position(u64);

impl Position {
    pub fn raw(&self) -> u64 {
        self.0
    }
}

impl From<Position> for protocol::streams::append_resp::Position {
    fn from(value: Position) -> Self {
        Self {
            commit_position: value.raw(),
            prepare_position: value.raw(),
        }
    }
}

impl From<String> for self::protocol::StreamIdentifier {
    fn from(value: String) -> Self {
        Self {
            stream_name: value.into_bytes(),
        }
    }
}

impl<'a> From<&'a str> for self::protocol::StreamIdentifier {
    fn from(value: &'a str) -> Self {
        Self {
            stream_name: value.as_bytes().to_owned(),
        }
    }
}

impl TryFrom<self::protocol::StreamIdentifier> for String {
    type Error = FromUtf8Error;

    fn try_from(value: self::protocol::StreamIdentifier) -> Result<Self, FromUtf8Error> {
        String::from_utf8(value.stream_name)
    }
}

impl From<protocol::streams::append_req::options::ExpectedStreamRevision> for ExpectedRevision {
    fn from(value: protocol::streams::append_req::options::ExpectedStreamRevision) -> Self {
        match value {
            protocol::streams::append_req::options::ExpectedStreamRevision::Any(_) => {
                ExpectedRevision::Any
            }
            protocol::streams::append_req::options::ExpectedStreamRevision::NoStream(_) => {
                ExpectedRevision::NoStream
            }
            protocol::streams::append_req::options::ExpectedStreamRevision::StreamExists(_) => {
                ExpectedRevision::StreamsExists
            }
            protocol::streams::append_req::options::ExpectedStreamRevision::Revision(v) => {
                ExpectedRevision::Revision(v)
            }
        }
    }
}

impl From<protocol::streams::RevisionOption> for Revision<u64> {
    fn from(value: protocol::streams::RevisionOption) -> Self {
        match value {
            protocol::streams::RevisionOption::Start(_) => Revision::Start,
            protocol::streams::RevisionOption::End(_) => Revision::End,
            protocol::streams::RevisionOption::Revision(v) => Revision::Revision(v),
        }
    }
}

impl From<protocol::streams::read_event::Position> for Position {
    fn from(value: protocol::streams::read_event::Position) -> Self {
        match value {
            protocol::streams::read_event::Position::CommitPosition(v) => Position(v),
            protocol::streams::read_event::Position::NoPosition(_) => Position(u64::MAX),
        }
    }
}

impl From<Position> for protocol::streams::read_event::Position {
    fn from(value: Position) -> Self {
        if value.raw() == u64::MAX {
            protocol::streams::read_event::Position::NoPosition(protocol::Empty::default())
        } else {
            protocol::streams::read_event::Position::CommitPosition(value.raw())
        }
    }
}
