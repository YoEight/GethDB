use std::{fmt::Display, string::FromUtf8Error};

use byteorder::{BigEndian, ByteOrder};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use thiserror::Error;
use uuid::Uuid;

pub use client::Client;
pub use io::{IteratorIO, IteratorIOExt};
use protocol::streams::append_resp;
use protocol::streams::delete_resp;

use crate::generated::next;
use crate::generated::next::protocol::operation_out::append_stream_completed::AppendResult;
use crate::generated::next::protocol::operation_out::{
    append_stream_completed, subscription_event,
};
use crate::generated::next::protocol::{delete_stream_completed, operation_in, operation_out};

mod client;
mod io;

mod google {
    pub mod rpc {
        include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
    }
}

pub mod generated {
    pub mod next {
        pub mod protocol {
            include!(concat!(env!("OUT_DIR"), "/geth.rs"));
        }
    }

    pub mod protocol {
        include!(concat!(env!("OUT_DIR"), "/event_store.client.rs"));
        pub mod streams {
            include!(concat!(env!("OUT_DIR"), "/event_store.client.streams.rs"));
        }
    }
}

pub mod protocol {
    pub use super::generated::protocol::{Empty, StreamIdentifier, Uuid};

    pub mod uuid {
        pub use super::super::generated::protocol::uuid::*;
    }

    pub mod streams {
        pub use super::super::generated::protocol::streams::read_req::options::{
            stream_options::RevisionOption, CountOption, StreamOption,
        };
        pub use super::super::generated::protocol::streams::*;
        pub use super::super::generated::protocol::streams::{
            append_req,
            append_resp::{self, success::CurrentRevisionOption, Success},
            read_resp::{
                self,
                read_event::{self, RecordedEvent},
                ReadEvent,
            },
        };

        pub mod server {
            pub use super::super::super::generated::protocol::streams::streams_server::*;
        }

        pub mod client {
            pub use super::super::super::generated::protocol::streams::streams_client::*;
        }
    }
}

impl From<Uuid> for next::protocol::Ident {
    fn from(value: Uuid) -> Self {
        let (most, least) = value.as_u64_pair();
        Self { most, least }
    }
}

impl From<next::protocol::Ident> for Uuid {
    fn from(value: next::protocol::Ident) -> Self {
        Uuid::from_u64_pair(value.most, value.least)
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

#[derive(Clone)]
pub struct EndPoint {
    pub host: String,
    pub port: u16,
}

#[derive(Clone)]
pub struct AppendStream {
    pub stream_name: String,
    pub events: Vec<Propose>,
    pub expected_revision: ExpectedRevision,
}

impl From<AppendStream> for operation_in::AppendStream {
    fn from(value: AppendStream) -> Self {
        Self {
            stream_name: value.stream_name,
            events: value.events.into_iter().map(|p| p.into()).collect(),
            expected_revision: Some(value.expected_revision.into()),
        }
    }
}

impl From<AppendStream> for operation_in::Operation {
    fn from(value: AppendStream) -> Self {
        operation_in::Operation::AppendStream(value.into())
    }
}

#[derive(Clone)]
pub struct DeleteStream {
    pub stream_name: String,
    pub expected_revision: ExpectedRevision,
}

impl From<DeleteStream> for operation_in::DeleteStream {
    fn from(value: DeleteStream) -> Self {
        Self {
            stream_name: value.stream_name,
            expected_revision: Some(value.expected_revision.into()),
        }
    }
}

impl From<DeleteStream> for operation_in::Operation {
    fn from(value: DeleteStream) -> Self {
        operation_in::Operation::DeleteStream(value.into())
    }
}

#[derive(Clone)]
pub struct ReadStream {
    pub stream_name: String,
    pub direction: Direction,
    pub revision: Revision<u64>,
    pub max_count: u64,
}

impl From<ReadStream> for operation_in::ReadStream {
    fn from(value: ReadStream) -> Self {
        Self {
            stream_name: value.stream_name,
            max_count: value.max_count,
            direction: Some(value.direction.into()),
            start: Some(value.revision.into()),
        }
    }
}

impl From<ReadStream> for operation_in::Operation {
    fn from(value: ReadStream) -> Self {
        operation_in::Operation::ReadStream(value.into())
    }
}

#[derive(Clone)]
pub enum Subscribe {
    ToProgram(SubscribeToProgram),
    ToStream(SubscribeToStream),
}

impl From<Subscribe> for operation_in::Subscribe {
    fn from(value: Subscribe) -> Self {
        match value {
            Subscribe::ToProgram(v) => operation_in::Subscribe {
                to: Some(operation_in::subscribe::To::Program(v.into())),
            },

            Subscribe::ToStream(v) => operation_in::Subscribe {
                to: Some(operation_in::subscribe::To::Stream(v.into())),
            },
        }
    }
}

impl From<Subscribe> for operation_in::Operation {
    fn from(value: Subscribe) -> Self {
        operation_in::Operation::Subscribe(value.into())
    }
}

#[derive(Clone)]
pub struct SubscribeToProgram {
    pub name: String,
    pub source: String,
}

impl From<SubscribeToProgram> for operation_in::subscribe::Program {
    fn from(value: SubscribeToProgram) -> Self {
        Self {
            name: value.name,
            source: value.source,
        }
    }
}

#[derive(Clone)]
pub struct SubscribeToStream {
    pub stream_name: String,
    pub start: Revision<u64>,
}

impl From<SubscribeToStream> for operation_in::subscribe::Stream {
    fn from(value: SubscribeToStream) -> Self {
        Self {
            stream_name: value.stream_name,
            start: Some(value.start.into()),
        }
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

impl From<Revision<u64>> for operation_in::read_stream::Start {
    fn from(value: Revision<u64>) -> Self {
        match value {
            Revision::Start => operation_in::read_stream::Start::Beginning(()),
            Revision::End => operation_in::read_stream::Start::End(()),
            Revision::Revision(r) => operation_in::read_stream::Start::Revision(r),
        }
    }
}

impl From<Revision<u64>> for operation_in::subscribe::stream::Start {
    fn from(value: Revision<u64>) -> Self {
        match value {
            Revision::Start => operation_in::subscribe::stream::Start::Beginning(()),
            Revision::End => operation_in::subscribe::stream::Start::End(()),
            Revision::Revision(r) => operation_in::subscribe::stream::Start::Revision(r),
        }
    }
}

impl From<append_stream_completed::error::wrong_expected_revision::CurrentRevision>
    for ExpectedRevision
{
    fn from(
        value: append_stream_completed::error::wrong_expected_revision::CurrentRevision,
    ) -> Self {
        match value {
            append_stream_completed::error::wrong_expected_revision::CurrentRevision::NotExists(
                _,
            ) => ExpectedRevision::NoStream,
            append_stream_completed::error::wrong_expected_revision::CurrentRevision::Revision(
                v,
            ) => ExpectedRevision::Revision(v),
        }
    }
}

impl From<delete_stream_completed::error::wrong_expected_revision::CurrentRevision>
    for ExpectedRevision
{
    fn from(
        value: delete_stream_completed::error::wrong_expected_revision::CurrentRevision,
    ) -> Self {
        match value {
            delete_stream_completed::error::wrong_expected_revision::CurrentRevision::NotExists(
                _,
            ) => ExpectedRevision::NoStream,
            delete_stream_completed::error::wrong_expected_revision::CurrentRevision::Revision(
                v,
            ) => ExpectedRevision::Revision(v),
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

impl From<Direction> for operation_in::read_stream::Direction {
    fn from(value: Direction) -> Self {
        match value {
            Direction::Forward => operation_in::read_stream::Direction::Forwards(()),
            Direction::Backward => operation_in::read_stream::Direction::Backwards(()),
        }
    }
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

impl From<Propose> for next::protocol::operation_in::append_stream::Propose {
    fn from(value: Propose) -> Self {
        Self {
            id: Some(value.id.into()),
            class: value.r#type,
            payload: value.data,
            metadata: Default::default(),
        }
    }
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

impl From<next::protocol::RecordedEvent> for Record {
    fn from(value: next::protocol::RecordedEvent) -> Self {
        Self {
            id: value.id.unwrap().into(),
            r#type: value.class,
            stream_name: value.stream_name,
            position: Position(value.position),
            revision: value.revision,
            data: value.payload,
        }
    }
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

impl From<ExpectedRevision> for operation_in::append_stream::ExpectedRevision {
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(r) => {
                operation_in::append_stream::ExpectedRevision::Revision(r)
            }
            ExpectedRevision::NoStream => {
                operation_in::append_stream::ExpectedRevision::NoStream(())
            }
            ExpectedRevision::Any => operation_in::append_stream::ExpectedRevision::Any(()),
            ExpectedRevision::StreamExists => {
                operation_in::append_stream::ExpectedRevision::StreamExists(())
            }
        }
    }
}

impl From<ExpectedRevision> for operation_in::delete_stream::ExpectedRevision {
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(r) => {
                operation_in::delete_stream::ExpectedRevision::Revision(r)
            }
            ExpectedRevision::NoStream => {
                operation_in::delete_stream::ExpectedRevision::NoStream(())
            }
            ExpectedRevision::Any => operation_in::delete_stream::ExpectedRevision::Any(()),
            ExpectedRevision::StreamExists => {
                operation_in::delete_stream::ExpectedRevision::StreamExists(())
            }
        }
    }
}

impl From<append_stream_completed::error::wrong_expected_revision::ExpectedRevision>
    for ExpectedRevision
{
    fn from(
        value: append_stream_completed::error::wrong_expected_revision::ExpectedRevision,
    ) -> Self {
        match value {
            append_stream_completed::error::wrong_expected_revision::ExpectedRevision::Any(_) => {
                ExpectedRevision::Any
            }
            append_stream_completed::error::wrong_expected_revision::ExpectedRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
            }
            append_stream_completed::error::wrong_expected_revision::ExpectedRevision::NoStream(
                _,
            ) => ExpectedRevision::NoStream,
            append_stream_completed::error::wrong_expected_revision::ExpectedRevision::Expected(
                v,
            ) => ExpectedRevision::Revision(v),
        }
    }
}

impl From<delete_stream_completed::error::wrong_expected_revision::ExpectedRevision>
    for ExpectedRevision
{
    fn from(
        value: delete_stream_completed::error::wrong_expected_revision::ExpectedRevision,
    ) -> Self {
        match value {
            delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::Any(_) => {
                ExpectedRevision::Any
            }
            delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
            }
            delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::NoStream(
                _,
            ) => ExpectedRevision::NoStream,
            delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::Expected(
                v,
            ) => ExpectedRevision::Revision(v),
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
    pub fn end() -> Self {
        Self(u64::MAX)
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

impl From<protocol::streams::delete_req::options::ExpectedStreamRevision> for ExpectedRevision {
    fn from(value: protocol::streams::delete_req::options::ExpectedStreamRevision) -> Self {
        match value {
            protocol::streams::delete_req::options::ExpectedStreamRevision::Any(_) => {
                ExpectedRevision::Any
            }
            protocol::streams::delete_req::options::ExpectedStreamRevision::NoStream(_) => {
                ExpectedRevision::NoStream
            }
            protocol::streams::delete_req::options::ExpectedStreamRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
            }
            protocol::streams::delete_req::options::ExpectedStreamRevision::Revision(v) => {
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

impl From<ExpectedRevision> for protocol::streams::delete_req::options::ExpectedStreamRevision {
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Any => {
                protocol::streams::delete_req::options::ExpectedStreamRevision::Any(
                    Default::default(),
                )
            }
            ExpectedRevision::NoStream => {
                protocol::streams::delete_req::options::ExpectedStreamRevision::NoStream(
                    Default::default(),
                )
            }
            ExpectedRevision::StreamExists => {
                protocol::streams::delete_req::options::ExpectedStreamRevision::StreamExists(
                    Default::default(),
                )
            }
            ExpectedRevision::Revision(v) => {
                protocol::streams::delete_req::options::ExpectedStreamRevision::Revision(v)
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

impl From<protocol::streams::delete_resp::Position> for Position {
    fn from(value: protocol::streams::delete_resp::Position) -> Self {
        Position(value.prepare_position)
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

impl From<ExpectedRevision> for delete_resp::wrong_expected_version::CurrentRevisionOption {
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                delete_resp::wrong_expected_version::CurrentRevisionOption::CurrentRevision(v)
            }
            ExpectedRevision::NoStream => {
                delete_resp::wrong_expected_version::CurrentRevisionOption::CurrentNoStream(
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

impl From<ExpectedRevision> for delete_resp::wrong_expected_version::ExpectedRevisionOption {
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                delete_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedRevision(v)
            }
            ExpectedRevision::NoStream => {
                delete_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedNoStream(
                    Default::default(),
                )
            }
            ExpectedRevision::Any => {
                delete_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedAny(
                    Default::default(),
                )
            }
            ExpectedRevision::StreamExists => {
                delete_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedStreamExists(
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

impl From<delete_resp::wrong_expected_version::CurrentRevisionOption> for ExpectedRevision {
    fn from(value: delete_resp::wrong_expected_version::CurrentRevisionOption) -> Self {
        match value {
            delete_resp::wrong_expected_version::CurrentRevisionOption::CurrentNoStream(_) => {
                ExpectedRevision::NoStream
            }
            delete_resp::wrong_expected_version::CurrentRevisionOption::CurrentRevision(v) => {
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

impl From<delete_resp::wrong_expected_version::ExpectedRevisionOption> for ExpectedRevision {
    fn from(value: delete_resp::wrong_expected_version::ExpectedRevisionOption) -> Self {
        match value {
            delete_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedAny(_) => {
                ExpectedRevision::Any
            }
            delete_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedNoStream(_) => {
                ExpectedRevision::NoStream
            }
            delete_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedStreamExists(
                _,
            ) => ExpectedRevision::StreamExists,
            delete_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedRevision(v) => {
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

#[derive(Debug)]
pub enum DeleteResult {
    WrongExpectedRevision(WrongExpectedRevisionError),
    Success(Position),
}

#[derive(Clone, Debug)]
pub struct ProgrammableStats {
    pub id: Uuid,
    pub name: String,
    pub source_code: String,
    pub subscriptions: Vec<String>,
    pub pushed_events: usize,
    pub started: DateTime<Utc>,
}

impl ProgrammableStats {
    pub fn new(id: Uuid, name: String, source_code: String) -> Self {
        Self {
            id,
            name,
            source_code,
            subscriptions: vec![],
            pushed_events: 0,
            started: Utc::now(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProgrammableSummary {
    pub id: Uuid,
    pub name: String,
    pub started: DateTime<Utc>,
}

pub enum AppendStreamCompleted {
    WriteResult(WriteResult),
    Error(AppendError),
}

pub enum AppendError {
    WrongExpectedRevision(WrongExpectedRevisionError),
}

impl From<operation_out::AppendStreamCompleted> for AppendStreamCompleted {
    fn from(value: operation_out::AppendStreamCompleted) -> Self {
        match value.append_result.unwrap() {
            AppendResult::WriteResult(r) => AppendStreamCompleted::WriteResult(WriteResult {
                next_expected_version: ExpectedRevision::Revision(r.next_revision),
                position: Position(r.position),
                next_logical_position: 0,
            }),

            AppendResult::Error(e) => match e.error.unwrap() {
                append_stream_completed::error::Error::WrongRevision(e) => {
                    AppendStreamCompleted::Error(AppendError::WrongExpectedRevision(
                        WrongExpectedRevisionError {
                            expected: e.expected_revision.unwrap().into(),
                            current: e.current_revision.unwrap().into(),
                        },
                    ))
                }
            },
        }
    }
}

pub enum StreamRead {
    EndOfStream,
    EventsAppeared(Vec<Record>),
    Error(StreamReadError),
}

impl From<operation_out::StreamRead> for StreamRead {
    fn from(value: operation_out::StreamRead) -> Self {
        match value.read_result.unwrap() {
            operation_out::stream_read::ReadResult::EndOfStream(_) => StreamRead::EndOfStream,
            operation_out::stream_read::ReadResult::EventsAppeared(e) => {
                StreamRead::EventsAppeared(e.events.into_iter().map(|r| r.into()).collect())
            }
            operation_out::stream_read::ReadResult::Error(_) => {
                StreamRead::Error(StreamReadError {})
            }
        }
    }
}

pub struct StreamReadError {}

impl Display for StreamReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamReadError")
    }
}

pub enum SubscriptionEvent {
    Confirmation(SubscriptionConfirmation),
    EventsAppeared(Vec<Record>),
    CaughtUp,
    Error(SubscriptionError),
}

pub enum SubscriptionConfirmation {
    StreamName(String),
    ProcessId(Uuid),
}

pub struct SubscriptionError {}

impl From<operation_out::SubscriptionEvent> for SubscriptionEvent {
    fn from(value: operation_out::SubscriptionEvent) -> Self {
        match value.event.unwrap() {
            subscription_event::Event::Confirmation(c) => match c.kind.unwrap() {
                subscription_event::confirmation::Kind::StreamName(s) => {
                    SubscriptionEvent::Confirmation(SubscriptionConfirmation::StreamName(s))
                }
                subscription_event::confirmation::Kind::ProcessId(p) => {
                    SubscriptionEvent::Confirmation(SubscriptionConfirmation::ProcessId(p.into()))
                }
            },
            subscription_event::Event::EventsAppeared(ea) => {
                SubscriptionEvent::EventsAppeared(ea.events.into_iter().map(|r| r.into()).collect())
            }
            subscription_event::Event::CaughtUp(_) => SubscriptionEvent::CaughtUp,
            subscription_event::Event::Error(e) => SubscriptionEvent::Error(e.into()),
        }
    }
}

impl From<subscription_event::Error> for SubscriptionError {
    fn from(_: subscription_event::Error) -> Self {
        SubscriptionError {}
    }
}

pub enum DeleteStreamCompleted {
    DeleteResult(WriteResult),
    Error(DeleteError),
}
pub enum DeleteError {
    WrongExpectedRevision(WrongExpectedRevisionError),
    NotLeaderException(EndPoint),
}

impl From<next::protocol::DeleteStreamCompleted> for DeleteStreamCompleted {
    fn from(value: next::protocol::DeleteStreamCompleted) -> Self {
        match value.result.unwrap() {
            delete_stream_completed::Result::WriteResult(r) => {
                DeleteStreamCompleted::DeleteResult(WriteResult {
                    next_expected_version: ExpectedRevision::Revision(r.next_revision),
                    position: Position(r.position),
                    next_logical_position: 0,
                })
            }

            delete_stream_completed::Result::Error(e) => match e.error.unwrap() {
                delete_stream_completed::error::Error::WrongRevision(e) => {
                    DeleteStreamCompleted::Error(DeleteError::WrongExpectedRevision(
                        WrongExpectedRevisionError {
                            expected: e.expected_revision.unwrap().into(),
                            current: e.current_revision.unwrap().into(),
                        },
                    ))
                }
                delete_stream_completed::error::Error::NotLeader(e) => {
                    DeleteStreamCompleted::Error(DeleteError::NotLeaderException(EndPoint {
                        host: e.leader_host,
                        port: e.leader_port as u16,
                    }))
                }
            },
        }
    }
}
