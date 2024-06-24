use std::{fmt::Display, string::FromUtf8Error};

use byteorder::{BigEndian, ByteOrder};
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use thiserror::Error;
use uuid::Uuid;

pub use client::{Client, SubscriptionEvent, UnsubscribeReason};
pub use io::{IteratorIO, IteratorIOExt};
use protocol::streams::append_resp;
use protocol::streams::delete_resp;

use crate::generated::next;
use crate::generated::next::protocol::{operation_in, operation_out};

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
pub struct OperationIn {
    pub correlation: Uuid,
    pub operation: Operation,
}

#[derive(Clone)]
pub enum Operation {
    AppendStream(AppendStream),
    DeleteStream(DeleteStream),
    ReadStream(ReadStream),
    Subscribe(Subscribe),
    ListPrograms(ListPrograms),
    GetProgram(GetProgram),
    KillProgram(KillProgram),
}

impl From<Operation> for operation_in::Operation {
    fn from(operation: Operation) -> Self {
        match operation {
            Operation::AppendStream(req) => operation_in::Operation::AppendStream(req.into()),
            Operation::DeleteStream(req) => operation_in::Operation::DeleteStream(req.into()),
            Operation::ReadStream(req) => operation_in::Operation::ReadStream(req.into()),
            Operation::Subscribe(req) => operation_in::Operation::Subscribe(req.into()),
            Operation::ListPrograms(req) => operation_in::Operation::ListPrograms(req.into()),
            Operation::GetProgram(req) => operation_in::Operation::GetProgram(req.into()),
            Operation::KillProgram(req) => operation_in::Operation::KillProgram(req.into()),
        }
    }
}
impl From<OperationIn> for next::protocol::OperationIn {
    fn from(operation: OperationIn) -> Self {
        let correlation = Some(operation.correlation.into());
        let operation = Some(operation.operation.into());

        Self {
            correlation,
            operation,
        }
    }
}

impl From<next::protocol::OperationIn> for OperationIn {
    fn from(operation: next::protocol::OperationIn) -> Self {
        let correlation = operation.correlation.unwrap().into();
        let operation = match operation.operation.unwrap() {
            operation_in::Operation::AppendStream(req) => Operation::AppendStream(req.into()),
            operation_in::Operation::DeleteStream(req) => Operation::DeleteStream(req.into()),
            operation_in::Operation::ReadStream(req) => Operation::ReadStream(req.into()),
            operation_in::Operation::Subscribe(req) => Operation::Subscribe(req.into()),
            operation_in::Operation::ListPrograms(req) => Operation::ListPrograms(req.into()),
            operation_in::Operation::GetProgram(req) => Operation::GetProgram(req.into()),
            operation_in::Operation::KillProgram(req) => Operation::KillProgram(req.into()),
        };

        Self {
            correlation,
            operation,
        }
    }
}

pub enum Reply {
    AppendStreamCompleted(AppendStreamCompleted),
    StreamRead(StreamRead),
    SubscriptionEvent(SubscriptionEventIR),
    DeleteStreamCompleted(DeleteStreamCompleted),
    ProgramsListed(ProgramListed),
    ProgramKilled(ProgramKilled),
    ProgramObtained(ProgramObtained),
}

pub struct OperationOut {
    pub correlation: Uuid,
    pub reply: Reply,
}

impl OperationOut {
    pub fn is_subscription_related(&self) -> bool {
        match &self.reply {
            Reply::SubscriptionEvent(event) => match event {
                SubscriptionEventIR::Error(_) => false,
                _ => true,
            },

            _ => false,
        }
    }
}

impl From<next::protocol::OperationOut> for OperationOut {
    fn from(value: next::protocol::OperationOut) -> Self {
        let correlation = value.correlation.unwrap().into();
        let reply = match value.operation.unwrap() {
            operation_out::Operation::AppendCompleted(resp) => {
                Reply::AppendStreamCompleted(resp.into())
            }
            operation_out::Operation::StreamRead(resp) => Reply::StreamRead(resp.into()),
            operation_out::Operation::SubscriptionEvent(resp) => {
                Reply::SubscriptionEvent(resp.into())
            }
            operation_out::Operation::DeleteCompleted(resp) => {
                Reply::DeleteStreamCompleted(resp.into())
            }
            operation_out::Operation::ProgramsListed(resp) => Reply::ProgramsListed(resp.into()),

            operation_out::Operation::ProgramKilled(resp) => Reply::ProgramKilled(resp.into()),

            operation_out::Operation::ProgramGot(resp) => Reply::ProgramObtained(resp.into()),
        };

        Self { correlation, reply }
    }
}

impl From<OperationOut> for next::protocol::OperationOut {
    fn from(value: OperationOut) -> Self {
        let correlation = Some(value.correlation.into());
        let operation = match value.reply {
            Reply::AppendStreamCompleted(resp) => {
                operation_out::Operation::AppendCompleted(resp.into())
            }
            Reply::StreamRead(resp) => operation_out::Operation::StreamRead(resp.into()),
            Reply::SubscriptionEvent(resp) => {
                operation_out::Operation::SubscriptionEvent(resp.into())
            }
            Reply::DeleteStreamCompleted(resp) => {
                operation_out::Operation::DeleteCompleted(resp.into())
            }
            Reply::ProgramsListed(resp) => operation_out::Operation::ProgramsListed(resp.into()),

            Reply::ProgramKilled(resp) => operation_out::Operation::ProgramKilled(resp.into()),

            Reply::ProgramObtained(resp) => operation_out::Operation::ProgramGot(resp.into()),
        };

        Self {
            correlation,
            operation: Some(operation),
        }
    }
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

impl From<operation_in::AppendStream> for AppendStream {
    fn from(value: operation_in::AppendStream) -> Self {
        Self {
            stream_name: value.stream_name,
            events: value.events.into_iter().map(|p| p.into()).collect(),
            expected_revision: value.expected_revision.unwrap().into(),
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

impl From<operation_in::DeleteStream> for DeleteStream {
    fn from(value: operation_in::DeleteStream) -> Self {
        Self {
            stream_name: value.stream_name,
            expected_revision: value.expected_revision.unwrap().into(),
        }
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

impl From<operation_in::ReadStream> for ReadStream {
    fn from(value: operation_in::ReadStream) -> Self {
        Self {
            stream_name: value.stream_name,
            direction: value.direction.unwrap().into(),
            revision: value.start.unwrap().into(),
            max_count: value.max_count,
        }
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

impl From<operation_in::Subscribe> for Subscribe {
    fn from(value: operation_in::Subscribe) -> Self {
        match value.to.unwrap() {
            operation_in::subscribe::To::Program(v) => Subscribe::ToProgram(v.into()),
            operation_in::subscribe::To::Stream(v) => Subscribe::ToStream(v.into()),
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

impl From<operation_in::subscribe::Program> for SubscribeToProgram {
    fn from(value: operation_in::subscribe::Program) -> Self {
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

impl From<operation_in::subscribe::Stream> for SubscribeToStream {
    fn from(value: operation_in::subscribe::Stream) -> Self {
        Self {
            stream_name: value.stream_name,
            start: value.start.unwrap().into(),
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

impl From<operation_in::read_stream::Start> for Revision<u64> {
    fn from(value: operation_in::read_stream::Start) -> Self {
        match value {
            operation_in::read_stream::Start::Beginning(_) => Revision::Start,
            operation_in::read_stream::Start::End(_) => Revision::End,
            operation_in::read_stream::Start::Revision(r) => Revision::Revision(r),
        }
    }
}

impl From<operation_in::subscribe::stream::Start> for Revision<u64> {
    fn from(value: operation_in::subscribe::stream::Start) -> Self {
        match value {
            operation_in::subscribe::stream::Start::Beginning(_) => Revision::Start,
            operation_in::subscribe::stream::Start::End(_) => Revision::End,
            operation_in::subscribe::stream::Start::Revision(r) => Revision::Revision(r),
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

impl From<operation_out::append_stream_completed::error::wrong_expected_revision::CurrentRevision>
    for ExpectedRevision
{
    fn from(
        value: operation_out::append_stream_completed::error::wrong_expected_revision::CurrentRevision,
    ) -> Self {
        match value {
            operation_out::append_stream_completed::error::wrong_expected_revision::CurrentRevision::NotExists(
                _,
            ) => ExpectedRevision::NoStream,
            operation_out::append_stream_completed::error::wrong_expected_revision::CurrentRevision::Revision(
                v,
            ) => ExpectedRevision::Revision(v),
        }
    }
}

impl From<next::protocol::delete_stream_completed::error::wrong_expected_revision::CurrentRevision>
    for ExpectedRevision
{
    fn from(
        value: next::protocol::delete_stream_completed::error::wrong_expected_revision::CurrentRevision,
    ) -> Self {
        match value {
            next::protocol::delete_stream_completed::error::wrong_expected_revision::CurrentRevision::NotExists(
                _,
            ) => ExpectedRevision::NoStream,
            next::protocol::delete_stream_completed::error::wrong_expected_revision::CurrentRevision::Revision(
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

impl From<operation_in::read_stream::Direction> for Direction {
    fn from(value: operation_in::read_stream::Direction) -> Self {
        match value {
            operation_in::read_stream::Direction::Forwards(_) => Direction::Forward,
            operation_in::read_stream::Direction::Backwards(_) => Direction::Backward,
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

impl From<Propose> for operation_in::append_stream::Propose {
    fn from(value: Propose) -> Self {
        Self {
            id: Some(value.id.into()),
            class: value.r#type,
            payload: value.data,
            metadata: Default::default(),
        }
    }
}

impl From<operation_in::append_stream::Propose> for Propose {
    fn from(value: operation_in::append_stream::Propose) -> Self {
        Self {
            id: value.id.unwrap().into(),
            r#type: value.class,
            data: value.payload,
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

impl From<Record> for next::protocol::RecordedEvent {
    fn from(value: Record) -> Self {
        Self {
            id: Some(value.id.into()),
            class: value.r#type,
            stream_name: value.stream_name,
            position: value.position.raw(),
            revision: value.revision,
            payload: value.data,
            metadata: Default::default(),
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
            ExpectedRevision::NoStream => write!(f, "'no stream'"),
            ExpectedRevision::Any => write!(f, "'any'"),
            ExpectedRevision::StreamExists => write!(f, "'stream exists'"),
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

impl From<ExpectedRevision>
    for operation_out::append_stream_completed::error::wrong_expected_revision::CurrentRevision
{
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                operation_out::append_stream_completed::error::wrong_expected_revision::CurrentRevision::Revision(v)
            }
            ExpectedRevision::NoStream => {
                operation_out::append_stream_completed::error::wrong_expected_revision::CurrentRevision::NotExists(())
            }
            _ => unreachable!(),
        }
    }
}

impl From<ExpectedRevision>
    for next::protocol::delete_stream_completed::error::wrong_expected_revision::CurrentRevision
{
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                next::protocol::delete_stream_completed::error::wrong_expected_revision::CurrentRevision::Revision(v)
            }
            ExpectedRevision::NoStream => {
                next::protocol::delete_stream_completed::error::wrong_expected_revision::CurrentRevision::NotExists(())
            }
            _ => unreachable!(),
        }
    }
}

impl From<ExpectedRevision>
    for operation_out::append_stream_completed::error::wrong_expected_revision::ExpectedRevision
{
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                operation_out::append_stream_completed::error::wrong_expected_revision::ExpectedRevision::Expected(v)
            }
            ExpectedRevision::NoStream => {
                operation_out::append_stream_completed::error::wrong_expected_revision::ExpectedRevision::NoStream(())
            }
            ExpectedRevision::Any => {
                operation_out::append_stream_completed::error::wrong_expected_revision::ExpectedRevision::Any(())
            }
            ExpectedRevision::StreamExists => {
                operation_out::append_stream_completed::error::wrong_expected_revision::ExpectedRevision::StreamExists(())
            }
        }
    }
}

impl From<ExpectedRevision>
    for next::protocol::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision
{
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                next::protocol::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::Expected(v)
            }
            ExpectedRevision::NoStream => {
                next::protocol::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::NoStream(())
            }
            ExpectedRevision::Any => {
                next::protocol::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::Any(())
            }
            ExpectedRevision::StreamExists => {
                next::protocol::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::StreamExists(())
            }
        }
    }
}

impl From<operation_in::append_stream::ExpectedRevision> for ExpectedRevision {
    fn from(value: operation_in::append_stream::ExpectedRevision) -> Self {
        match value {
            operation_in::append_stream::ExpectedRevision::Revision(r) => {
                ExpectedRevision::Revision(r)
            }
            operation_in::append_stream::ExpectedRevision::NoStream(_) => {
                ExpectedRevision::NoStream
            }
            operation_in::append_stream::ExpectedRevision::Any(_) => ExpectedRevision::Any,
            operation_in::append_stream::ExpectedRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
            }
        }
    }
}

impl From<operation_in::delete_stream::ExpectedRevision> for ExpectedRevision {
    fn from(value: operation_in::delete_stream::ExpectedRevision) -> Self {
        match value {
            operation_in::delete_stream::ExpectedRevision::Revision(r) => {
                ExpectedRevision::Revision(r)
            }
            operation_in::delete_stream::ExpectedRevision::NoStream(_) => {
                ExpectedRevision::NoStream
            }
            operation_in::delete_stream::ExpectedRevision::Any(_) => ExpectedRevision::Any,
            operation_in::delete_stream::ExpectedRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
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

impl From<operation_out::append_stream_completed::error::wrong_expected_revision::ExpectedRevision>
    for ExpectedRevision
{
    fn from(
        value: operation_out::append_stream_completed::error::wrong_expected_revision::ExpectedRevision,
    ) -> Self {
        match value {
            operation_out::append_stream_completed::error::wrong_expected_revision::ExpectedRevision::Any(_) => {
                ExpectedRevision::Any
            }
            operation_out::append_stream_completed::error::wrong_expected_revision::ExpectedRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
            }
            operation_out::append_stream_completed::error::wrong_expected_revision::ExpectedRevision::NoStream(
                _,
            ) => ExpectedRevision::NoStream,
            operation_out::append_stream_completed::error::wrong_expected_revision::ExpectedRevision::Expected(
                v,
            ) => ExpectedRevision::Revision(v),
        }
    }
}

impl From<next::protocol::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision>
    for ExpectedRevision
{
    fn from(
        value: next::protocol::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision,
    ) -> Self {
        match value {
            next::protocol::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::Any(_) => {
                ExpectedRevision::Any
            }
            next::protocol::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
            }
            next::protocol::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::NoStream(
                _,
            ) => ExpectedRevision::NoStream,
            next::protocol::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::Expected(
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

impl From<WrongExpectedRevisionError>
    for operation_out::append_stream_completed::error::WrongExpectedRevision
{
    fn from(value: WrongExpectedRevisionError) -> Self {
        Self {
            current_revision: Some(value.current.into()),
            expected_revision: Some(value.expected.into()),
        }
    }
}

impl From<WrongExpectedRevisionError>
    for next::protocol::delete_stream_completed::error::WrongExpectedRevision
{
    fn from(value: WrongExpectedRevisionError) -> Self {
        Self {
            current_revision: Some(value.current.into()),
            expected_revision: Some(value.expected.into()),
        }
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

impl From<WriteResult> for operation_out::append_stream_completed::WriteResult {
    fn from(value: WriteResult) -> Self {
        Self {
            next_revision: value.next_expected_version.raw() as u64,
            position: value.position.raw(),
        }
    }
}

impl From<WriteResult> for next::protocol::delete_stream_completed::DeleteResult {
    fn from(value: WriteResult) -> Self {
        Self {
            next_revision: value.next_expected_version.raw() as u64,
            position: value.position.raw(),
        }
    }
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

impl Display for AppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppendError::WrongExpectedRevision(e) => {
                write!(
                    f,
                    "expected revision {} but got {} instead",
                    e.expected, e.current
                )
            }
        }
    }
}

impl From<operation_out::AppendStreamCompleted> for AppendStreamCompleted {
    fn from(value: operation_out::AppendStreamCompleted) -> Self {
        match value.append_result.unwrap() {
            operation_out::append_stream_completed::AppendResult::WriteResult(r) => {
                AppendStreamCompleted::WriteResult(WriteResult {
                    next_expected_version: ExpectedRevision::Revision(r.next_revision),
                    position: Position(r.position),
                    next_logical_position: 0,
                })
            }

            operation_out::append_stream_completed::AppendResult::Error(e) => {
                match e.error.unwrap() {
                    operation_out::append_stream_completed::error::Error::WrongRevision(e) => {
                        AppendStreamCompleted::Error(AppendError::WrongExpectedRevision(
                            WrongExpectedRevisionError {
                                expected: e.expected_revision.unwrap().into(),
                                current: e.current_revision.unwrap().into(),
                            },
                        ))
                    }
                }
            }
        }
    }
}

impl From<AppendStreamCompleted> for operation_out::AppendStreamCompleted {
    fn from(value: AppendStreamCompleted) -> Self {
        match value {
            AppendStreamCompleted::WriteResult(w) => operation_out::AppendStreamCompleted {
                append_result: Some(
                    operation_out::append_stream_completed::AppendResult::WriteResult(w.into()),
                ),
            },

            AppendStreamCompleted::Error(e) => operation_out::AppendStreamCompleted {
                append_result: Some(operation_out::append_stream_completed::AppendResult::Error(
                    operation_out::append_stream_completed::Error {
                        error: Some(match e {
                            AppendError::WrongExpectedRevision(e) => {
                                operation_out::append_stream_completed::error::Error::WrongRevision(
                                    e.into(),
                                )
                            }
                        }),
                    },
                )),
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

impl From<StreamRead> for operation_out::StreamRead {
    fn from(value: StreamRead) -> Self {
        match value {
            StreamRead::EndOfStream => operation_out::StreamRead {
                read_result: Some(operation_out::stream_read::ReadResult::EndOfStream(())),
            },
            StreamRead::EventsAppeared(e) => operation_out::StreamRead {
                read_result: Some(operation_out::stream_read::ReadResult::EventsAppeared(
                    operation_out::stream_read::EventsAppeared {
                        events: e.into_iter().map(|r| r.into()).collect(),
                    },
                )),
            },
            StreamRead::Error(_) => operation_out::StreamRead {
                read_result: Some(operation_out::stream_read::ReadResult::Error(
                    operation_out::stream_read::Error {},
                )),
            },
        }
    }
}

pub struct StreamReadError {}

impl Display for StreamReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamReadError")
    }
}

pub enum SubscriptionEventIR {
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

impl Display for SubscriptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SubscriptionError")
    }
}

impl From<operation_out::SubscriptionEvent> for SubscriptionEventIR {
    fn from(value: operation_out::SubscriptionEvent) -> Self {
        match value.event.unwrap() {
            operation_out::subscription_event::Event::Confirmation(c) => match c.kind.unwrap() {
                operation_out::subscription_event::confirmation::Kind::StreamName(s) => {
                    SubscriptionEventIR::Confirmation(SubscriptionConfirmation::StreamName(s))
                }
                operation_out::subscription_event::confirmation::Kind::ProcessId(p) => {
                    SubscriptionEventIR::Confirmation(SubscriptionConfirmation::ProcessId(p.into()))
                }
            },
            operation_out::subscription_event::Event::EventsAppeared(ea) => {
                SubscriptionEventIR::EventsAppeared(
                    ea.events.into_iter().map(|r| r.into()).collect(),
                )
            }
            operation_out::subscription_event::Event::CaughtUp(_) => SubscriptionEventIR::CaughtUp,
            operation_out::subscription_event::Event::Error(e) => {
                SubscriptionEventIR::Error(e.into())
            }
        }
    }
}

impl From<SubscriptionEventIR> for operation_out::SubscriptionEvent {
    fn from(value: SubscriptionEventIR) -> Self {
        match value {
            SubscriptionEventIR::Confirmation(c) => match c {
                SubscriptionConfirmation::StreamName(s) => operation_out::SubscriptionEvent {
                    event: Some(operation_out::subscription_event::Event::Confirmation(
                        operation_out::subscription_event::Confirmation {
                            kind: Some(
                                operation_out::subscription_event::confirmation::Kind::StreamName(
                                    s,
                                ),
                            ),
                        },
                    )),
                },
                SubscriptionConfirmation::ProcessId(p) => operation_out::SubscriptionEvent {
                    event: Some(operation_out::subscription_event::Event::Confirmation(
                        operation_out::subscription_event::Confirmation {
                            kind: Some(
                                operation_out::subscription_event::confirmation::Kind::ProcessId(
                                    p.into(),
                                ),
                            ),
                        },
                    )),
                },
            },
            SubscriptionEventIR::EventsAppeared(ea) => operation_out::SubscriptionEvent {
                event: Some(operation_out::subscription_event::Event::EventsAppeared(
                    operation_out::subscription_event::EventsAppeared {
                        events: ea.into_iter().map(|r| r.into()).collect(),
                    },
                )),
            },
            SubscriptionEventIR::CaughtUp => operation_out::SubscriptionEvent {
                event: Some(operation_out::subscription_event::Event::CaughtUp(
                    operation_out::subscription_event::CaughtUp {},
                )),
            },
            SubscriptionEventIR::Error(e) => operation_out::SubscriptionEvent {
                event: Some(operation_out::subscription_event::Event::Error(
                    operation_out::subscription_event::Error {},
                )),
            },
        }
    }
}

impl From<operation_out::subscription_event::Error> for SubscriptionError {
    fn from(_: operation_out::subscription_event::Error) -> Self {
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

impl Display for DeleteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeleteError::WrongExpectedRevision(e) => {
                write!(
                    f,
                    "expected revision {} but got {} instead",
                    e.expected, e.current
                )
            }
            DeleteError::NotLeaderException(e) => {
                write!(f, "Not leader exception: {}:{}", e.host, e.port)
            }
        }
    }
}

impl From<next::protocol::DeleteStreamCompleted> for DeleteStreamCompleted {
    fn from(value: next::protocol::DeleteStreamCompleted) -> Self {
        match value.result.unwrap() {
            next::protocol::delete_stream_completed::Result::WriteResult(r) => {
                DeleteStreamCompleted::DeleteResult(WriteResult {
                    next_expected_version: ExpectedRevision::Revision(r.next_revision),
                    position: Position(r.position),
                    next_logical_position: 0,
                })
            }

            next::protocol::delete_stream_completed::Result::Error(e) => match e.error.unwrap() {
                next::protocol::delete_stream_completed::error::Error::WrongRevision(e) => {
                    DeleteStreamCompleted::Error(DeleteError::WrongExpectedRevision(
                        WrongExpectedRevisionError {
                            expected: e.expected_revision.unwrap().into(),
                            current: e.current_revision.unwrap().into(),
                        },
                    ))
                }
                next::protocol::delete_stream_completed::error::Error::NotLeader(e) => {
                    DeleteStreamCompleted::Error(DeleteError::NotLeaderException(EndPoint {
                        host: e.leader_host,
                        port: e.leader_port as u16,
                    }))
                }
            },
        }
    }
}

impl From<DeleteStreamCompleted> for next::protocol::DeleteStreamCompleted {
    fn from(value: DeleteStreamCompleted) -> Self {
        match value {
            DeleteStreamCompleted::DeleteResult(w) => next::protocol::DeleteStreamCompleted {
                result: Some(
                    next::protocol::delete_stream_completed::Result::WriteResult(w.into()),
                ),
            },

            DeleteStreamCompleted::Error(e) => next::protocol::DeleteStreamCompleted {
                result: Some(next::protocol::delete_stream_completed::Result::Error(
                    next::protocol::delete_stream_completed::Error {
                        error: Some(match e {
                            DeleteError::WrongExpectedRevision(e) => {
                                next::protocol::delete_stream_completed::error::Error::WrongRevision(
                                    e.into(),
                                )
                            }
                            DeleteError::NotLeaderException(e) => {
                                next::protocol::delete_stream_completed::error::Error::NotLeader(
                                    next::protocol::delete_stream_completed::error::NotLeader {
                                        leader_host: e.host,
                                        leader_port: e.port as u32,
                                    },
                                )
                            }
                        }),
                    },
                )),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct ListPrograms {}

impl From<ListPrograms> for operation_in::ListPrograms {
    fn from(_: ListPrograms) -> Self {
        Self { empty: None }
    }
}

impl From<operation_in::ListPrograms> for ListPrograms {
    fn from(_: operation_in::ListPrograms) -> Self {
        Self {}
    }
}

#[derive(Clone, Debug)]
pub struct GetProgram {
    pub id: Uuid,
}

impl From<GetProgram> for operation_in::GetProgram {
    fn from(value: GetProgram) -> Self {
        Self {
            id: Some(value.id.into()),
        }
    }
}

impl From<operation_in::GetProgram> for GetProgram {
    fn from(value: operation_in::GetProgram) -> Self {
        Self {
            id: value.id.unwrap().into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct KillProgram {
    pub id: Uuid,
}

impl From<KillProgram> for operation_in::KillProgram {
    fn from(value: KillProgram) -> Self {
        Self {
            id: Some(value.id.into()),
        }
    }
}

impl From<operation_in::KillProgram> for KillProgram {
    fn from(value: operation_in::KillProgram) -> Self {
        Self {
            id: value.id.unwrap().into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProgramListed {
    pub programs: Vec<ProgramSummary>,
}

#[derive(Clone, Debug)]
pub struct ProgramSummary {
    pub id: Uuid,
    pub name: String,
    pub started_at: DateTime<Utc>,
}

impl From<operation_out::programs_listed::ProgramSummary> for ProgramSummary {
    fn from(value: operation_out::programs_listed::ProgramSummary) -> Self {
        Self {
            id: value.id.unwrap().into(),
            name: value.name,
            started_at: Utc.timestamp_opt(value.started_at, 0).unwrap(),
        }
    }
}

impl From<ProgramSummary> for operation_out::programs_listed::ProgramSummary {
    fn from(value: ProgramSummary) -> Self {
        Self {
            id: Some(value.id.into()),
            name: value.name,
            started_at: value.started_at.timestamp(),
        }
    }
}

impl From<operation_out::ProgramsListed> for ProgramListed {
    fn from(value: operation_out::ProgramsListed) -> Self {
        Self {
            programs: value.programs.into_iter().map(|p| p.into()).collect(),
        }
    }
}

impl From<ProgramListed> for operation_out::ProgramsListed {
    fn from(value: ProgramListed) -> Self {
        Self {
            programs: value.programs.into_iter().map(|p| p.into()).collect(),
        }
    }
}

pub enum ProgramKilled {
    Success,
    Error(eyre::Report),
}

impl From<operation_out::ProgramKilled> for ProgramKilled {
    fn from(value: operation_out::ProgramKilled) -> Self {
        match value.result.unwrap() {
            operation_out::program_killed::Result::Success(_) => ProgramKilled::Success,
            operation_out::program_killed::Result::Error(e) => match e.error.unwrap() {
                operation_out::program_killed::error::Error::NotExists(_) => {
                    ProgramKilled::Error(eyre::eyre!("program not exists"))
                }
            },
        }
    }
}

impl From<ProgramKilled> for operation_out::ProgramKilled {
    fn from(value: ProgramKilled) -> Self {
        match value {
            ProgramKilled::Success => operation_out::ProgramKilled {
                result: Some(operation_out::program_killed::Result::Success(())),
            },

            ProgramKilled::Error(e) => operation_out::ProgramKilled {
                result: Some(operation_out::program_killed::Result::Error(
                    operation_out::program_killed::Error {
                        error: Some(operation_out::program_killed::error::Error::NotExists(())),
                    },
                )),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProgramStats {
    pub id: Uuid,
    pub name: String,
    pub source_code: String,
    pub subscriptions: Vec<String>,
    pub pushed_events: usize,
    pub started: DateTime<Utc>,
}

impl From<operation_out::program_obtained::ProgramStats> for ProgramStats {
    fn from(value: operation_out::program_obtained::ProgramStats) -> Self {
        Self {
            id: value.id.unwrap().into(),
            name: value.name,
            source_code: value.source_code,
            subscriptions: value.subscriptions,
            pushed_events: value.pushed_events as usize,
            started: Utc.timestamp_opt(value.started_at, 0).unwrap(),
        }
    }
}

impl From<ProgramStats> for operation_out::program_obtained::ProgramStats {
    fn from(value: ProgramStats) -> Self {
        Self {
            id: Some(value.id.into()),
            name: value.name,
            source_code: value.source_code,
            subscriptions: value.subscriptions,
            pushed_events: value.pushed_events as u64,
            started_at: value.started.timestamp(),
        }
    }
}

#[derive(Debug)]
pub enum ProgramObtained {
    Success(ProgramStats),
    Error(eyre::Report),
}

impl From<operation_out::ProgramObtained> for ProgramObtained {
    fn from(value: operation_out::ProgramObtained) -> Self {
        match value.result.unwrap() {
            operation_out::program_obtained::Result::Program(stats) => {
                ProgramObtained::Success(stats.into())
            }

            operation_out::program_obtained::Result::Error(e) => match e.error.unwrap() {
                operation_out::program_obtained::error::Error::NotExists(_) => {
                    ProgramObtained::Error(eyre::eyre!("program not found"))
                }
            },
        }
    }
}

impl From<ProgramObtained> for operation_out::ProgramObtained {
    fn from(value: ProgramObtained) -> Self {
        match value {
            ProgramObtained::Success(stats) => operation_out::ProgramObtained {
                result: Some(operation_out::program_obtained::Result::Program(
                    stats.into(),
                )),
            },

            ProgramObtained::Error(e) => operation_out::ProgramObtained {
                result: Some(operation_out::program_obtained::Result::Error(
                    operation_out::program_obtained::Error {
                        error: Some(operation_out::program_obtained::error::Error::NotExists(())),
                    },
                )),
            },
        }
    }
}
