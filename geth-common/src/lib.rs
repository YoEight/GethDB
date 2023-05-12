use std::{fmt::Display, string::FromUtf8Error};

use byteorder::{BigEndian, ByteOrder};
use bytes::Bytes;
use protocol::streams::append_resp;
use thiserror::Error;
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

        pub mod client {
            pub use super::super::super::geth::protocol::streams::streams_client::*;
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

impl Revision<u64> {
    pub fn is_greater_than(&self, rev: u64) -> bool {
        match self {
            Revision::Start => false,
            Revision::End => true,
            Revision::Revision(point) => *point > rev,
        }
    }

    pub fn raw(&self) -> u64 {
        match self {
            Revision::Start => 0,
            Revision::End => u64::MAX,
            Revision::Revision(r) => *r,
        }
    }
}

impl<D: Display> Display for Revision<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Revision::Start => write!(f, "Start"),
            Revision::End => write!(f, "End"),
            Revision::Revision(v) => write!(f, "{}", v),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

impl From<Direction> for i32 {
    fn from(value: Direction) -> Self {
        match value {
            Direction::Forward => 0,
            Direction::Backward => 1,
        }
    }
}

pub struct WrongDirectionError;

#[derive(Debug, Clone)]
pub struct Propose {
    pub id: Uuid,
    pub r#type: String,
    pub data: Bytes,
}

#[derive(Debug)]
pub struct Record {
    pub id: Uuid,
    pub r#type: String,
    pub stream_name: String,
    pub position: Position,
    pub revision: u64,
    pub data: Bytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ExpectedRevision {
    Revision(u64),
    NoStream,
    Any,
    StreamExists,
}

impl ExpectedRevision {
    pub fn raw(&self) -> i64 {
        match self {
            ExpectedRevision::Revision(v) => *v as i64,
            ExpectedRevision::NoStream => -1,
            ExpectedRevision::Any => -2,
            ExpectedRevision::StreamExists => -3,
        }
    }
}

impl Display for ExpectedRevision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExpectedRevision::Revision(v) => write!(f, "{}", v),
            ExpectedRevision::NoStream => write!(f, "NoStream"),
            ExpectedRevision::Any => write!(f, "Any"),
            ExpectedRevision::StreamExists => write!(f, "StreamExists"),
        }
    }
}

#[derive(Error, Debug)]
pub struct WrongExpectedRevisionError {
    pub expected: ExpectedRevision,
    pub current: ExpectedRevision,
}

impl Display for WrongExpectedRevisionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Expected revision {} but got {} instead",
            self.expected, self.current
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Position(pub u64);

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
                ExpectedRevision::StreamExists
            }
            protocol::streams::append_req::options::ExpectedStreamRevision::Revision(v) => {
                ExpectedRevision::Revision(v)
            }
        }
    }
}

impl From<ExpectedRevision> for protocol::streams::append_req::options::ExpectedStreamRevision {
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Any => {
                protocol::streams::append_req::options::ExpectedStreamRevision::Any(
                    Default::default(),
                )
            }
            ExpectedRevision::NoStream => {
                protocol::streams::append_req::options::ExpectedStreamRevision::NoStream(
                    Default::default(),
                )
            }
            ExpectedRevision::StreamExists => {
                protocol::streams::append_req::options::ExpectedStreamRevision::StreamExists(
                    Default::default(),
                )
            }
            ExpectedRevision::Revision(v) => {
                protocol::streams::append_req::options::ExpectedStreamRevision::Revision(v)
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

impl From<Revision<u64>> for protocol::streams::RevisionOption {
    fn from(value: Revision<u64>) -> Self {
        match value {
            Revision::Start => protocol::streams::RevisionOption::Start(Default::default()),
            Revision::End => protocol::streams::RevisionOption::End(Default::default()),
            Revision::Revision(v) => protocol::streams::RevisionOption::Revision(v),
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

impl From<append_resp::success::PositionOption> for Position {
    fn from(value: append_resp::success::PositionOption) -> Self {
        match value {
            append_resp::success::PositionOption::NoPosition(_) => Position(0),
            append_resp::success::PositionOption::Position(pos) => Position(pos.prepare_position),
        }
    }
}

impl From<append_resp::success::CurrentRevisionOption> for ExpectedRevision {
    fn from(value: append_resp::success::CurrentRevisionOption) -> Self {
        match value {
            append_resp::success::CurrentRevisionOption::CurrentRevision(v) => {
                ExpectedRevision::Revision(v)
            }
            append_resp::success::CurrentRevisionOption::NoStream(_) => ExpectedRevision::NoStream,
        }
    }
}

impl From<ExpectedRevision> for append_resp::success::CurrentRevisionOption {
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                append_resp::success::CurrentRevisionOption::CurrentRevision(v)
            }
            ExpectedRevision::NoStream => {
                append_resp::success::CurrentRevisionOption::NoStream(Default::default())
            }
            _ => unreachable!(),
        }
    }
}

impl From<ExpectedRevision> for append_resp::wrong_expected_version::CurrentRevisionOption {
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                append_resp::wrong_expected_version::CurrentRevisionOption::CurrentRevision(v)
            }
            ExpectedRevision::NoStream => {
                append_resp::wrong_expected_version::CurrentRevisionOption::CurrentNoStream(
                    Default::default(),
                )
            }
            _ => unreachable!(),
        }
    }
}

impl From<ExpectedRevision> for append_resp::wrong_expected_version::ExpectedRevisionOption {
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                append_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedRevision(v)
            }
            ExpectedRevision::NoStream => {
                append_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedNoStream(
                    Default::default(),
                )
            }
            ExpectedRevision::Any => {
                append_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedAny(
                    Default::default(),
                )
            }
            ExpectedRevision::StreamExists => {
                append_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedStreamExists(
                    Default::default(),
                )
            }
        }
    }
}

impl From<append_resp::wrong_expected_version::CurrentRevisionOption> for ExpectedRevision {
    fn from(value: append_resp::wrong_expected_version::CurrentRevisionOption) -> Self {
        match value {
            append_resp::wrong_expected_version::CurrentRevisionOption::CurrentNoStream(_) => {
                ExpectedRevision::NoStream
            }
            append_resp::wrong_expected_version::CurrentRevisionOption::CurrentRevision(v) => {
                ExpectedRevision::Revision(v)
            }
        }
    }
}

impl From<append_resp::wrong_expected_version::ExpectedRevisionOption> for ExpectedRevision {
    fn from(value: append_resp::wrong_expected_version::ExpectedRevisionOption) -> Self {
        match value {
            append_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedAny(_) => {
                ExpectedRevision::Any
            }
            append_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedNoStream(_) => {
                ExpectedRevision::NoStream
            }
            append_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedStreamExists(
                _,
            ) => ExpectedRevision::StreamExists,
            append_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedRevision(v) => {
                ExpectedRevision::Revision(v)
            }
        }
    }
}

impl From<Option<u64>> for protocol::streams::CurrentRevisionOption {
    fn from(value: Option<u64>) -> Self {
        if let Some(v) = value {
            protocol::streams::CurrentRevisionOption::CurrentRevision(v)
        } else {
            protocol::streams::CurrentRevisionOption::NoStream(protocol::Empty::default())
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct WriteResult {
    pub next_expected_version: ExpectedRevision,
    pub position: Position,
    pub next_logical_position: u64,
}
