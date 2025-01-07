use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::any::type_name;
use std::fmt::Display;
use thiserror::Error;
use uuid::Uuid;

pub use client::{Client, SubscriptionEvent, UnsubscribeReason};
pub use io::{IteratorIO, IteratorIOExt};

use crate::generated::next;
use crate::generated::next::protocol::{operation_in, operation_out};

mod client;
mod io;

pub mod generated {
    pub mod next {
        pub mod protocol {
            include!(concat!(env!("OUT_DIR"), "/geth.rs"));
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

#[derive(Clone)]
pub struct EndPoint {
    pub host: String,
    pub port: u16,
}

impl EndPoint {
    pub fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }
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
    Unsubscribe,
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
            Operation::Unsubscribe => operation_in::Operation::Unsubscribe(()),
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
            operation_in::Operation::Unsubscribe(_) => Operation::Unsubscribe,
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
    SubscriptionEvent(SubscriptionEvent),
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
    pub fn is_streaming(&self) -> bool {
        match &self.reply {
            Reply::SubscriptionEvent(event) => !matches!(event, SubscriptionEvent::Unsubscribed(_)),
            Reply::StreamRead(event) => matches!(event, StreamRead::EventAppeared(_)),
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

impl From<operation_out::delete_stream_completed::error::wrong_expected_revision::CurrentRevision>
    for ExpectedRevision
{
    fn from(
        value: operation_out::delete_stream_completed::error::wrong_expected_revision::CurrentRevision,
    ) -> Self {
        match value {
            operation_out::delete_stream_completed::error::wrong_expected_revision::CurrentRevision::NotExists(
                _,
            ) => ExpectedRevision::NoStream,
            operation_out::delete_stream_completed::error::wrong_expected_revision::CurrentRevision::Revision(
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ContentType {
    Unknown = 0,
    Json = 1,
    Binary = 2,
}

impl TryFrom<i32> for ContentType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ContentType::Unknown),
            1 => Ok(ContentType::Json),
            _ => Err(()),
        }
    }
}

impl From<next::protocol::ContentType> for ContentType {
    fn from(value: next::protocol::ContentType) -> Self {
        match value {
            next::protocol::ContentType::Unknown => Self::Unknown,
            next::protocol::ContentType::Json => Self::Json,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Propose {
    pub id: Uuid,
    pub content_type: ContentType,
    pub class: String,
    pub data: Bytes,
}

impl Propose {
    pub fn from_value<A>(value: &A) -> eyre::Result<Self>
    where
        A: Serialize,
    {
        let data = Bytes::from(serde_json::to_vec(value)?);
        let id = Uuid::new_v4();
        Ok(Self {
            id,
            content_type: ContentType::Json,
            class: type_name::<A>().to_string(),
            data,
        })
    }
}

impl From<Propose> for operation_in::append_stream::Propose {
    fn from(value: Propose) -> Self {
        Self {
            id: Some(value.id.into()),
            content_type: value.content_type as i32,
            class: value.class,
            payload: value.data,
            metadata: Default::default(),
        }
    }
}

impl From<operation_in::append_stream::Propose> for Propose {
    fn from(value: operation_in::append_stream::Propose) -> Self {
        Self {
            id: value.id.unwrap().into(),
            content_type: next::protocol::ContentType::try_from(value.content_type)
                .map(ContentType::from)
                .unwrap_or(ContentType::Unknown),
            class: value.class,
            data: value.payload,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Record {
    pub id: Uuid,
    pub content_type: ContentType,
    pub class: String,
    pub stream_name: String,
    pub position: Position,
    pub revision: u64,
    pub data: Bytes,
}

impl From<next::protocol::RecordedEvent> for Record {
    fn from(value: next::protocol::RecordedEvent) -> Self {
        Self {
            id: value.id.unwrap().into(),
            content_type: next::protocol::ContentType::try_from(value.content_type)
                .map(ContentType::from)
                .unwrap_or(ContentType::Unknown),
            stream_name: value.stream_name,
            class: value.class,
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
            content_type: value.content_type as i32,
            stream_name: value.stream_name,
            class: value.class,
            position: value.position.raw(),
            revision: value.revision,
            payload: value.data,
            metadata: Default::default(),
        }
    }
}
impl Record {
    pub fn as_value<'a, A>(&'a self) -> eyre::Result<A>
    where
        A: Deserialize<'a>,
    {
        let value = serde_json::from_slice(&self.data)?;
        Ok(value)
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
    for operation_out::delete_stream_completed::error::wrong_expected_revision::CurrentRevision
{
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                operation_out::delete_stream_completed::error::wrong_expected_revision::CurrentRevision::Revision(v)
            }
            ExpectedRevision::NoStream => {
                operation_out::delete_stream_completed::error::wrong_expected_revision::CurrentRevision::NotExists(())
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
    for operation_out::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision
{
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                operation_out::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::Expected(v)
            }
            ExpectedRevision::NoStream => {
                operation_out::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::NoStream(())
            }
            ExpectedRevision::Any => {
                operation_out::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::Any(())
            }
            ExpectedRevision::StreamExists => {
                operation_out::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::StreamExists(())
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

impl From<operation_out::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision>
    for ExpectedRevision
{
    fn from(
        value: operation_out::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision,
    ) -> Self {
        match value {
            operation_out::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::Any(_) => {
                ExpectedRevision::Any
            }
            operation_out::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
            }
            operation_out::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::NoStream(
                _,
            ) => ExpectedRevision::NoStream,
            operation_out::delete_stream_completed::error::wrong_expected_revision::ExpectedRevision::Expected(
                v,
            ) => ExpectedRevision::Revision(v),
        }
    }
}
#[derive(Error, Debug, Copy, Clone)]
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
    for operation_out::delete_stream_completed::error::WrongExpectedRevision
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

#[derive(Clone, Copy, Debug)]
pub enum AppendCompleted {
    Success(WriteResult),
    Error(WrongExpectedRevisionError),
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

impl From<WriteResult> for operation_out::delete_stream_completed::DeleteResult {
    fn from(value: WriteResult) -> Self {
        Self {
            next_revision: value.next_expected_version.raw() as u64,
            position: value.position.raw(),
        }
    }
}

#[derive(Debug)]
pub enum AppendStreamCompleted {
    Success(WriteResult),
    Error(AppendError),
}

impl AppendStreamCompleted {
    pub fn err(self) -> eyre::Result<AppendError> {
        if let Self::Error(e) = self {
            return Ok(e);
        }

        eyre::bail!("append succeeded")
    }

    pub fn success(self) -> eyre::Result<WriteResult> {
        if let Self::Success(r) = self {
            return Ok(r);
        }

        eyre::bail!("append failed")
    }
}

#[derive(Clone, Copy, Debug)]
pub enum AppendError {
    WrongExpectedRevision(WrongExpectedRevisionError),
    StreamDeleted,
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

            AppendError::StreamDeleted => write!(f, "stream deleted"),
        }
    }
}

impl From<operation_out::AppendStreamCompleted> for AppendStreamCompleted {
    fn from(value: operation_out::AppendStreamCompleted) -> Self {
        match value.append_result.unwrap() {
            operation_out::append_stream_completed::AppendResult::WriteResult(r) => {
                AppendStreamCompleted::Success(WriteResult {
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
                    operation_out::append_stream_completed::error::Error::StreamDeleted(_) => {
                        AppendStreamCompleted::Error(AppendError::StreamDeleted)
                    }
                }
            }
        }
    }
}

impl From<AppendStreamCompleted> for operation_out::AppendStreamCompleted {
    fn from(value: AppendStreamCompleted) -> Self {
        match value {
            AppendStreamCompleted::Success(w) => operation_out::AppendStreamCompleted {
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
                            AppendError::StreamDeleted => {
                                operation_out::append_stream_completed::error::Error::StreamDeleted(
                                    (),
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
    EventAppeared(Record),
    Error(StreamReadError),
}

impl From<operation_out::StreamRead> for StreamRead {
    fn from(value: operation_out::StreamRead) -> Self {
        match value.read_result.unwrap() {
            operation_out::stream_read::ReadResult::EndOfStream(_) => StreamRead::EndOfStream,
            operation_out::stream_read::ReadResult::EventAppeared(e) => {
                StreamRead::EventAppeared(e.event.unwrap().into())
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
            StreamRead::EventAppeared(e) => operation_out::StreamRead {
                read_result: Some(operation_out::stream_read::ReadResult::EventAppeared(
                    operation_out::stream_read::EventAppeared {
                        event: Some(e.into()),
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

pub struct StreamReadError;

impl Display for StreamReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamReadError")
    }
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

impl From<operation_out::SubscriptionEvent> for SubscriptionEvent {
    fn from(value: operation_out::SubscriptionEvent) -> Self {
        match value.event.unwrap() {
            operation_out::subscription_event::Event::Confirmation(c) => match c.kind.unwrap() {
                operation_out::subscription_event::confirmation::Kind::StreamName(s) => {
                    SubscriptionEvent::Confirmed(SubscriptionConfirmation::StreamName(s))
                }
                operation_out::subscription_event::confirmation::Kind::ProcessId(p) => {
                    SubscriptionEvent::Confirmed(SubscriptionConfirmation::ProcessId(p.into()))
                }
            },
            operation_out::subscription_event::Event::EventAppeared(e) => {
                SubscriptionEvent::EventAppeared(e.event.unwrap().into())
            }
            operation_out::subscription_event::Event::CaughtUp(_) => SubscriptionEvent::CaughtUp,
            operation_out::subscription_event::Event::Error(_) => {
                SubscriptionEvent::Unsubscribed(UnsubscribeReason::Server)
            }
        }
    }
}

impl From<SubscriptionEvent> for operation_out::SubscriptionEvent {
    fn from(value: SubscriptionEvent) -> Self {
        match value {
            SubscriptionEvent::Confirmed(c) => match c {
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
            SubscriptionEvent::EventAppeared(e) => operation_out::SubscriptionEvent {
                event: Some(operation_out::subscription_event::Event::EventAppeared(
                    operation_out::subscription_event::EventAppeared {
                        event: Some(e.into()),
                    },
                )),
            },
            SubscriptionEvent::CaughtUp => operation_out::SubscriptionEvent {
                event: Some(operation_out::subscription_event::Event::CaughtUp(
                    operation_out::subscription_event::CaughtUp {},
                )),
            },
            SubscriptionEvent::Unsubscribed(_) => operation_out::SubscriptionEvent {
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
    Success(WriteResult),
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

impl From<operation_out::DeleteStreamCompleted> for DeleteStreamCompleted {
    fn from(value: operation_out::DeleteStreamCompleted) -> Self {
        match value.result.unwrap() {
            operation_out::delete_stream_completed::Result::WriteResult(r) => {
                DeleteStreamCompleted::Success(WriteResult {
                    next_expected_version: ExpectedRevision::Revision(r.next_revision),
                    position: Position(r.position),
                    next_logical_position: 0,
                })
            }

            operation_out::delete_stream_completed::Result::Error(e) => match e.error.unwrap() {
                operation_out::delete_stream_completed::error::Error::WrongRevision(e) => {
                    DeleteStreamCompleted::Error(DeleteError::WrongExpectedRevision(
                        WrongExpectedRevisionError {
                            expected: e.expected_revision.unwrap().into(),
                            current: e.current_revision.unwrap().into(),
                        },
                    ))
                }
                operation_out::delete_stream_completed::error::Error::NotLeader(e) => {
                    DeleteStreamCompleted::Error(DeleteError::NotLeaderException(EndPoint {
                        host: e.leader_host,
                        port: e.leader_port as u16,
                    }))
                }
            },
        }
    }
}

impl From<DeleteStreamCompleted> for operation_out::DeleteStreamCompleted {
    fn from(value: DeleteStreamCompleted) -> Self {
        match value {
            DeleteStreamCompleted::Success(w) => operation_out::DeleteStreamCompleted {
                result: Some(operation_out::delete_stream_completed::Result::WriteResult(
                    w.into(),
                )),
            },

            DeleteStreamCompleted::Error(e) => operation_out::DeleteStreamCompleted {
                result: Some(operation_out::delete_stream_completed::Result::Error(
                    operation_out::delete_stream_completed::Error {
                        error: Some(match e {
                            DeleteError::WrongExpectedRevision(e) => {
                                operation_out::delete_stream_completed::error::Error::WrongRevision(
                                    e.into(),
                                )
                            }
                            DeleteError::NotLeaderException(e) => {
                                operation_out::delete_stream_completed::error::Error::NotLeader(
                                    operation_out::delete_stream_completed::error::NotLeader {
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

#[derive(Clone, Copy, Debug)]
pub enum ProgramKilled {
    Success,
    Error(ProgramKillError),
}

#[derive(Clone, Copy, Debug)]
pub enum ProgramKillError {
    NotExists,
}

impl From<operation_out::ProgramKilled> for ProgramKilled {
    fn from(value: operation_out::ProgramKilled) -> Self {
        match value.result.unwrap() {
            operation_out::program_killed::Result::Success(_) => ProgramKilled::Success,
            operation_out::program_killed::Result::Error(e) => match e.error.unwrap() {
                operation_out::program_killed::error::Error::NotExists(_) => {
                    ProgramKilled::Error(ProgramKillError::NotExists)
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
                        error: Some(match e {
                            ProgramKillError::NotExists => {
                                operation_out::program_killed::error::Error::NotExists(())
                            }
                        }),
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
    Error(GetProgramError),
}

#[derive(Debug)]
pub enum GetProgramError {
    NotExists,
}

impl From<operation_out::ProgramObtained> for ProgramObtained {
    fn from(value: operation_out::ProgramObtained) -> Self {
        match value.result.unwrap() {
            operation_out::program_obtained::Result::Program(stats) => {
                ProgramObtained::Success(stats.into())
            }

            operation_out::program_obtained::Result::Error(e) => match e.error.unwrap() {
                operation_out::program_obtained::error::Error::NotExists(_) => {
                    ProgramObtained::Error(GetProgramError::NotExists)
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
                        error: Some(match e {
                            GetProgramError::NotExists => {
                                operation_out::program_obtained::error::Error::NotExists(())
                            }
                        }),
                    },
                )),
            },
        }
    }
}

#[derive(Clone)]
pub enum ReadCompleted<A> {
    Success(A),
    StreamDeleted,
}

impl<A> ReadCompleted<A> {
    pub fn ok(self) -> eyre::Result<A> {
        if let ReadCompleted::Success(result) = self {
            return Ok(result);
        }

        eyre::bail!("stream was deleted when trying to read from it")
    }
}
