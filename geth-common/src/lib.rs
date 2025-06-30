use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::any::type_name;
use std::fmt::Display;
use thiserror::Error;
use uuid::Uuid;

pub use client::{SubscriptionEvent, SubscriptionNotification, UnsubscribeReason};
pub use io::{IteratorIO, IteratorIOExt};

mod client;
mod io;

pub use crate::generated::protocol;

pub mod generated {
    pub mod protocol {
        include!(concat!(env!("OUT_DIR"), "/geth.rs"));
    }
}

impl From<Uuid> for protocol::Ident {
    fn from(value: Uuid) -> Self {
        let (most, least) = value.as_u64_pair();
        Self { most, least }
    }
}

impl From<protocol::Ident> for Uuid {
    fn from(value: protocol::Ident) -> Self {
        Uuid::from_u64_pair(value.most, value.least)
    }
}

#[derive(Clone, Debug)]
pub struct EndPoint {
    pub host: String,
    pub port: u16,
}

impl EndPoint {
    pub fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }
}

impl Display for EndPoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
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
    GetProgramStats(GetProgramStats),
    KillProgram(KillProgram),
    Unsubscribe,
}

#[derive(Debug)]
pub enum Reply {
    AppendStreamCompleted(AppendStreamCompleted),
    StreamRead(ReadStreamResponse),
    SubscriptionEvent(SubscriptionEvent),
    DeleteStreamCompleted(DeleteStreamCompleted),
    ProgramsListed(ProgramListed),
    ProgramKilled(ProgramKilled),
    ProgramObtained(ProgramObtained),
    ServerDisconnected,
    Error(String),
}

pub struct OperationOut {
    pub correlation: Uuid,
    pub reply: Reply,
}

impl OperationOut {
    pub fn is_streaming(&self) -> bool {
        match &self.reply {
            Reply::SubscriptionEvent(event) => !matches!(event, SubscriptionEvent::Unsubscribed(_)),
            Reply::StreamRead(event) => matches!(event, ReadStreamResponse::EventAppeared(_)),
            _ => false,
        }
    }
}

#[derive(Clone)]
pub struct AppendStream {
    pub stream_name: String,
    pub events: Vec<Propose>,
    pub expected_revision: ExpectedRevision,
}

impl From<AppendStream> for protocol::AppendStreamRequest {
    fn from(value: AppendStream) -> Self {
        Self {
            stream_name: value.stream_name,
            events: value.events.into_iter().map(|p| p.into()).collect(),
            expected_revision: Some(value.expected_revision.into()),
        }
    }
}

impl From<protocol::AppendStreamRequest> for AppendStream {
    fn from(value: protocol::AppendStreamRequest) -> Self {
        Self {
            stream_name: value.stream_name,
            events: value.events.into_iter().map(|p| p.into()).collect(),
            expected_revision: value.expected_revision.unwrap().into(),
        }
    }
}

#[derive(Clone)]
pub struct DeleteStream {
    pub stream_name: String,
    pub expected_revision: ExpectedRevision,
}

impl From<DeleteStream> for protocol::DeleteStreamRequest {
    fn from(value: DeleteStream) -> Self {
        Self {
            stream_name: value.stream_name,
            expected_revision: Some(value.expected_revision.into()),
        }
    }
}

impl From<protocol::DeleteStreamRequest> for DeleteStream {
    fn from(value: protocol::DeleteStreamRequest) -> Self {
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

impl From<ReadStream> for protocol::ReadStreamRequest {
    fn from(value: ReadStream) -> Self {
        Self {
            stream_name: value.stream_name,
            max_count: value.max_count,
            direction: Some(value.direction.into()),
            start: Some(value.revision.into()),
        }
    }
}

impl From<protocol::ReadStreamRequest> for ReadStream {
    fn from(value: protocol::ReadStreamRequest) -> Self {
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

impl From<Subscribe> for protocol::SubscribeRequest {
    fn from(value: Subscribe) -> Self {
        match value {
            Subscribe::ToProgram(v) => protocol::SubscribeRequest {
                to: Some(protocol::subscribe_request::To::Program(v.into())),
            },

            Subscribe::ToStream(v) => protocol::SubscribeRequest {
                to: Some(protocol::subscribe_request::To::Stream(v.into())),
            },
        }
    }
}

impl From<protocol::SubscribeRequest> for Subscribe {
    fn from(value: protocol::SubscribeRequest) -> Self {
        match value.to.unwrap() {
            protocol::subscribe_request::To::Program(v) => Subscribe::ToProgram(v.into()),
            protocol::subscribe_request::To::Stream(v) => Subscribe::ToStream(v.into()),
        }
    }
}

#[derive(Clone)]
pub struct SubscribeToProgram {
    pub name: String,
    pub source: String,
}

impl From<SubscribeToProgram> for protocol::subscribe_request::Program {
    fn from(value: SubscribeToProgram) -> Self {
        Self {
            name: value.name,
            source: value.source,
        }
    }
}

impl From<protocol::subscribe_request::Program> for SubscribeToProgram {
    fn from(value: protocol::subscribe_request::Program) -> Self {
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

impl From<SubscribeToStream> for protocol::subscribe_request::Stream {
    fn from(value: SubscribeToStream) -> Self {
        Self {
            stream_name: value.stream_name,
            start: Some(value.start.into()),
        }
    }
}

impl From<protocol::subscribe_request::Stream> for SubscribeToStream {
    fn from(value: protocol::subscribe_request::Stream) -> Self {
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

impl From<Revision<u64>> for protocol::read_stream_request::Start {
    fn from(value: Revision<u64>) -> Self {
        match value {
            Revision::Start => protocol::read_stream_request::Start::Beginning(()),
            Revision::End => protocol::read_stream_request::Start::End(()),
            Revision::Revision(r) => protocol::read_stream_request::Start::Revision(r),
        }
    }
}

impl From<protocol::read_stream_request::Start> for Revision<u64> {
    fn from(value: protocol::read_stream_request::Start) -> Self {
        match value {
            protocol::read_stream_request::Start::Beginning(_) => Revision::Start,
            protocol::read_stream_request::Start::End(_) => Revision::End,
            protocol::read_stream_request::Start::Revision(r) => Revision::Revision(r),
        }
    }
}

impl From<protocol::subscribe_request::stream::Start> for Revision<u64> {
    fn from(value: protocol::subscribe_request::stream::Start) -> Self {
        match value {
            protocol::subscribe_request::stream::Start::Beginning(_) => Revision::Start,
            protocol::subscribe_request::stream::Start::End(_) => Revision::End,
            protocol::subscribe_request::stream::Start::Revision(r) => Revision::Revision(r),
        }
    }
}

impl From<Revision<u64>> for protocol::subscribe_request::stream::Start {
    fn from(value: Revision<u64>) -> Self {
        match value {
            Revision::Start => protocol::subscribe_request::stream::Start::Beginning(()),
            Revision::End => protocol::subscribe_request::stream::Start::End(()),
            Revision::Revision(r) => protocol::subscribe_request::stream::Start::Revision(r),
        }
    }
}

impl From<protocol::append_stream_response::error::wrong_expected_revision::CurrentRevision>
    for ExpectedRevision
{
    fn from(
        value: protocol::append_stream_response::error::wrong_expected_revision::CurrentRevision,
    ) -> Self {
        match value {
            protocol::append_stream_response::error::wrong_expected_revision::CurrentRevision::NotExists(
                _,
            ) => ExpectedRevision::NoStream,
            protocol::append_stream_response::error::wrong_expected_revision::CurrentRevision::Revision(
                v,
            ) => ExpectedRevision::Revision(v),
        }
    }
}

impl From<protocol::delete_stream_response::error::wrong_expected_revision::CurrentRevision>
    for ExpectedRevision
{
    fn from(
        value: protocol::delete_stream_response::error::wrong_expected_revision::CurrentRevision,
    ) -> Self {
        match value {
            protocol::delete_stream_response::error::wrong_expected_revision::CurrentRevision::NotExists(
                _,
            ) => ExpectedRevision::NoStream,
            protocol::delete_stream_response::error::wrong_expected_revision::CurrentRevision::Revision(
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
            Revision::Revision(v) => write!(f, "{v}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Backward,
}

impl From<Direction> for protocol::read_stream_request::Direction {
    fn from(value: Direction) -> Self {
        match value {
            Direction::Forward => protocol::read_stream_request::Direction::Forwards(()),
            Direction::Backward => protocol::read_stream_request::Direction::Backwards(()),
        }
    }
}

impl From<protocol::read_stream_request::Direction> for Direction {
    fn from(value: protocol::read_stream_request::Direction) -> Self {
        match value {
            protocol::read_stream_request::Direction::Forwards(_) => Direction::Forward,
            protocol::read_stream_request::Direction::Backwards(_) => Direction::Backward,
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
    type Error = eyre::Report;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ContentType::Unknown),
            1 => Ok(ContentType::Json),
            2 => Ok(ContentType::Binary),
            x => eyre::bail!("unknown content type: {}", x),
        }
    }
}

impl From<protocol::ContentType> for ContentType {
    fn from(value: protocol::ContentType) -> Self {
        match value {
            protocol::ContentType::Unknown => Self::Unknown,
            protocol::ContentType::Json => Self::Json,
            protocol::ContentType::Binary => Self::Binary,
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

impl From<Propose> for protocol::append_stream_request::Propose {
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

impl From<protocol::append_stream_request::Propose> for Propose {
    fn from(value: protocol::append_stream_request::Propose) -> Self {
        Self {
            id: value.id.unwrap().into(),
            content_type: protocol::ContentType::try_from(value.content_type)
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
    pub position: u64,
    pub revision: u64,
    pub data: Bytes,
}

impl From<protocol::RecordedEvent> for Record {
    fn from(value: protocol::RecordedEvent) -> Self {
        Self {
            id: value.id.unwrap().into(),
            content_type: protocol::ContentType::try_from(value.content_type)
                .map(ContentType::from)
                .unwrap_or(ContentType::Unknown),
            stream_name: value.stream_name,
            class: value.class,
            position: value.position,
            revision: value.revision,
            data: value.payload,
        }
    }
}

impl From<Record> for protocol::RecordedEvent {
    fn from(value: Record) -> Self {
        Self {
            id: Some(value.id.into()),
            content_type: value.content_type as i32,
            stream_name: value.stream_name,
            class: value.class,
            position: value.position,
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

    pub fn as_pyro_value<'a, A>(&'a self) -> eyre::Result<PyroRecord<A>>
    where
        A: Deserialize<'a>,
    {
        self.as_value::<PyroRecord<A>>()
    }
}

#[derive(Serialize, Deserialize)]
pub struct PyroRecord<A> {
    pub class: String,
    pub event_revision: u64,
    pub id: Uuid,
    pub position: u64,
    pub stream_name: String,
    pub payload: A,
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
            ExpectedRevision::Revision(v) => write!(f, "{v}"),
            ExpectedRevision::NoStream => write!(f, "'no stream'"),
            ExpectedRevision::Any => write!(f, "'any'"),
            ExpectedRevision::StreamExists => write!(f, "'stream exists'"),
        }
    }
}

impl From<ExpectedRevision> for protocol::append_stream_request::ExpectedRevision {
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(r) => {
                protocol::append_stream_request::ExpectedRevision::Revision(r)
            }
            ExpectedRevision::NoStream => {
                protocol::append_stream_request::ExpectedRevision::NoStream(())
            }
            ExpectedRevision::Any => protocol::append_stream_request::ExpectedRevision::Any(()),
            ExpectedRevision::StreamExists => {
                protocol::append_stream_request::ExpectedRevision::StreamExists(())
            }
        }
    }
}

impl From<ExpectedRevision>
    for protocol::append_stream_response::error::wrong_expected_revision::CurrentRevision
{
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                protocol::append_stream_response::error::wrong_expected_revision::CurrentRevision::Revision(v)
            }
            ExpectedRevision::NoStream => {
                protocol::append_stream_response::error::wrong_expected_revision::CurrentRevision::NotExists(())
            }
            _ => unreachable!(),
        }
    }
}

impl From<ExpectedRevision>
    for protocol::delete_stream_response::error::wrong_expected_revision::CurrentRevision
{
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                protocol::delete_stream_response::error::wrong_expected_revision::CurrentRevision::Revision(v)
            }
            ExpectedRevision::NoStream => {
                protocol::delete_stream_response::error::wrong_expected_revision::CurrentRevision::NotExists(())
            }
            _ => unreachable!(),
        }
    }
}

impl From<ExpectedRevision>
    for protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision
{
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::Expected(v)
            }
            ExpectedRevision::NoStream => {
                protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::NoStream(())
            }
            ExpectedRevision::Any => {
                protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::Any(())
            }
            ExpectedRevision::StreamExists => {
                protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::StreamExists(())
            }
        }
    }
}

impl From<ExpectedRevision>
    for protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision
{
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::Expected(v)
            }
            ExpectedRevision::NoStream => {
                protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::NoStream(())
            }
            ExpectedRevision::Any => {
                protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::Any(())
            }
            ExpectedRevision::StreamExists => {
                protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::StreamExists(())
            }
        }
    }
}

impl From<protocol::append_stream_request::ExpectedRevision> for ExpectedRevision {
    fn from(value: protocol::append_stream_request::ExpectedRevision) -> Self {
        match value {
            protocol::append_stream_request::ExpectedRevision::Revision(r) => {
                ExpectedRevision::Revision(r)
            }
            protocol::append_stream_request::ExpectedRevision::NoStream(_) => {
                ExpectedRevision::NoStream
            }
            protocol::append_stream_request::ExpectedRevision::Any(_) => ExpectedRevision::Any,
            protocol::append_stream_request::ExpectedRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
            }
        }
    }
}

impl From<protocol::delete_stream_request::ExpectedRevision> for ExpectedRevision {
    fn from(value: protocol::delete_stream_request::ExpectedRevision) -> Self {
        match value {
            protocol::delete_stream_request::ExpectedRevision::Revision(r) => {
                ExpectedRevision::Revision(r)
            }
            protocol::delete_stream_request::ExpectedRevision::NoStream(_) => {
                ExpectedRevision::NoStream
            }
            protocol::delete_stream_request::ExpectedRevision::Any(_) => ExpectedRevision::Any,
            protocol::delete_stream_request::ExpectedRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
            }
        }
    }
}

impl From<ExpectedRevision> for protocol::delete_stream_request::ExpectedRevision {
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(r) => {
                protocol::delete_stream_request::ExpectedRevision::Revision(r)
            }
            ExpectedRevision::NoStream => {
                protocol::delete_stream_request::ExpectedRevision::NoStream(())
            }
            ExpectedRevision::Any => protocol::delete_stream_request::ExpectedRevision::Any(()),
            ExpectedRevision::StreamExists => {
                protocol::delete_stream_request::ExpectedRevision::StreamExists(())
            }
        }
    }
}

impl From<protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision>
    for ExpectedRevision
{
    fn from(
        value: protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision,
    ) -> Self {
        match value {
            protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::Any(_) => {
                ExpectedRevision::Any
            }
            protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
            }
            protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::NoStream(
                _,
            ) => ExpectedRevision::NoStream,
            protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::Expected(
                v,
            ) => ExpectedRevision::Revision(v),
        }
    }
}

impl From<protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision>
    for ExpectedRevision
{
    fn from(
        value: protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision,
    ) -> Self {
        match value {
            protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::Any(_) => {
                ExpectedRevision::Any
            }
            protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
            }
            protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::NoStream(
                _,
            ) => ExpectedRevision::NoStream,
            protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::Expected(
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
    for protocol::append_stream_response::error::WrongExpectedRevision
{
    fn from(value: WrongExpectedRevisionError) -> Self {
        Self {
            current_revision: Some(value.current.into()),
            expected_revision: Some(value.expected.into()),
        }
    }
}

impl From<WrongExpectedRevisionError>
    for protocol::delete_stream_response::error::WrongExpectedRevision
{
    fn from(value: WrongExpectedRevisionError) -> Self {
        Self {
            current_revision: Some(value.current.into()),
            expected_revision: Some(value.expected.into()),
        }
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
    pub position: u64,
    pub next_logical_position: u64,
}

impl From<WriteResult> for protocol::append_stream_response::WriteResult {
    fn from(value: WriteResult) -> Self {
        Self {
            next_revision: value.next_expected_version.raw() as u64,
            position: value.position,
        }
    }
}

impl From<WriteResult> for protocol::delete_stream_response::DeleteResult {
    fn from(value: WriteResult) -> Self {
        Self {
            next_revision: value.next_expected_version.raw() as u64,
            position: value.position,
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

impl From<protocol::AppendStreamResponse> for AppendStreamCompleted {
    fn from(value: protocol::AppendStreamResponse) -> Self {
        match value.append_result.unwrap() {
            protocol::append_stream_response::AppendResult::WriteResult(r) => {
                AppendStreamCompleted::Success(WriteResult {
                    next_expected_version: ExpectedRevision::Revision(r.next_revision),
                    position: r.position,
                    next_logical_position: 0,
                })
            }

            protocol::append_stream_response::AppendResult::Error(e) => match e.error.unwrap() {
                protocol::append_stream_response::error::Error::WrongRevision(e) => {
                    AppendStreamCompleted::Error(AppendError::WrongExpectedRevision(
                        WrongExpectedRevisionError {
                            expected: e.expected_revision.unwrap().into(),
                            current: e.current_revision.unwrap().into(),
                        },
                    ))
                }
                protocol::append_stream_response::error::Error::StreamDeleted(_) => {
                    AppendStreamCompleted::Error(AppendError::StreamDeleted)
                }
            },
        }
    }
}

impl From<AppendStreamCompleted> for protocol::AppendStreamResponse {
    fn from(value: AppendStreamCompleted) -> Self {
        match value {
            AppendStreamCompleted::Success(w) => protocol::AppendStreamResponse {
                append_result: Some(protocol::append_stream_response::AppendResult::WriteResult(
                    w.into(),
                )),
            },

            AppendStreamCompleted::Error(e) => protocol::AppendStreamResponse {
                append_result: Some(protocol::append_stream_response::AppendResult::Error(
                    protocol::append_stream_response::Error {
                        error: Some(match e {
                            AppendError::WrongExpectedRevision(e) => {
                                protocol::append_stream_response::error::Error::WrongRevision(
                                    e.into(),
                                )
                            }
                            AppendError::StreamDeleted => {
                                protocol::append_stream_response::error::Error::StreamDeleted(())
                            }
                        }),
                    },
                )),
            },
        }
    }
}

pub enum ReadStreamCompleted<A> {
    StreamDeleted,
    Success(A),
}

impl<A> ReadStreamCompleted<A> {
    pub fn success(self) -> eyre::Result<A> {
        match self {
            ReadStreamCompleted::StreamDeleted => eyre::bail!("stream deleted"),
            ReadStreamCompleted::Success(a) => Ok(a),
        }
    }

    pub fn is_stream_deleted(&self) -> bool {
        matches!(self, ReadStreamCompleted::StreamDeleted)
    }
}

#[derive(Debug)]
pub enum ReadStreamResponse {
    EndOfStream,
    EventAppeared(Record),
    StreamDeleted,
}

impl From<protocol::ReadStreamResponse> for ReadStreamResponse {
    fn from(value: protocol::ReadStreamResponse) -> Self {
        match value.read_result.unwrap() {
            protocol::read_stream_response::ReadResult::EndOfStream(_) => {
                ReadStreamResponse::EndOfStream
            }
            protocol::read_stream_response::ReadResult::EventAppeared(e) => {
                ReadStreamResponse::EventAppeared(e.into())
            }
        }
    }
}

impl TryFrom<ReadStreamResponse> for protocol::ReadStreamResponse {
    type Error = ReadError;

    fn try_from(value: ReadStreamResponse) -> Result<Self, Self::Error> {
        match value {
            ReadStreamResponse::EndOfStream => Ok(protocol::ReadStreamResponse {
                read_result: Some(protocol::read_stream_response::ReadResult::EndOfStream(())),
            }),

            ReadStreamResponse::EventAppeared(e) => Ok(protocol::ReadStreamResponse {
                read_result: Some(protocol::read_stream_response::ReadResult::EventAppeared(
                    e.into(),
                )),
            }),

            ReadStreamResponse::StreamDeleted => Err(ReadError::StreamDeleted),
        }
    }
}

#[derive(Debug)]
pub enum ReadError {
    StreamDeleted,
}

#[derive(Debug, Clone)]
pub enum SubscriptionConfirmation {
    StreamName(String),
    ProcessId(u64),
}

impl SubscriptionConfirmation {
    pub fn try_into_process_id(self) -> eyre::Result<u64> {
        match self {
            SubscriptionConfirmation::StreamName(_) => {
                eyre::bail!("unexpected a program confirmation but got a stream instead")
            }
            SubscriptionConfirmation::ProcessId(id) => Ok(id),
        }
    }
}

pub struct SubscriptionError {}

impl Display for SubscriptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SubscriptionError")
    }
}

impl From<protocol::subscribe_response::Notification> for SubscriptionNotification {
    fn from(value: protocol::subscribe_response::Notification) -> Self {
        match value.kind.unwrap() {
            protocol::subscribe_response::notification::Kind::Subscribed(s) => Self::Subscribed(s),
            protocol::subscribe_response::notification::Kind::Unsubscribed(s) => {
                Self::Unsubscribed(s)
            }
        }
    }
}

impl From<SubscriptionNotification> for protocol::subscribe_response::Notification {
    fn from(value: SubscriptionNotification) -> Self {
        protocol::subscribe_response::Notification {
            kind: Some(match value {
                SubscriptionNotification::Subscribed(s) => {
                    protocol::subscribe_response::notification::Kind::Subscribed(s)
                }

                SubscriptionNotification::Unsubscribed(s) => {
                    protocol::subscribe_response::notification::Kind::Unsubscribed(s)
                }
            }),
        }
    }
}

impl From<protocol::SubscribeResponse> for SubscriptionEvent {
    fn from(value: protocol::SubscribeResponse) -> Self {
        match value.event.unwrap() {
            protocol::subscribe_response::Event::Confirmation(c) => match c.kind.unwrap() {
                protocol::subscribe_response::confirmation::Kind::StreamName(s) => {
                    SubscriptionEvent::Confirmed(SubscriptionConfirmation::StreamName(s))
                }
                protocol::subscribe_response::confirmation::Kind::ProcessId(p) => {
                    SubscriptionEvent::Confirmed(SubscriptionConfirmation::ProcessId(p))
                }
            },
            protocol::subscribe_response::Event::EventAppeared(e) => {
                SubscriptionEvent::EventAppeared(e.event.unwrap().into())
            }
            protocol::subscribe_response::Event::CaughtUp(_) => SubscriptionEvent::CaughtUp,
            protocol::subscribe_response::Event::Error(_) => {
                SubscriptionEvent::Unsubscribed(UnsubscribeReason::Server)
            }
            protocol::subscribe_response::Event::Notification(n) => {
                SubscriptionEvent::Notification(n.into())
            }
        }
    }
}

impl From<SubscriptionEvent> for protocol::SubscribeResponse {
    fn from(value: SubscriptionEvent) -> Self {
        match value {
            SubscriptionEvent::Confirmed(c) => match c {
                SubscriptionConfirmation::StreamName(s) => protocol::SubscribeResponse {
                    event: Some(protocol::subscribe_response::Event::Confirmation(
                        protocol::subscribe_response::Confirmation {
                            kind: Some(
                                protocol::subscribe_response::confirmation::Kind::StreamName(s),
                            ),
                        },
                    )),
                },
                SubscriptionConfirmation::ProcessId(p) => protocol::SubscribeResponse {
                    event: Some(protocol::subscribe_response::Event::Confirmation(
                        protocol::subscribe_response::Confirmation {
                            kind: Some(
                                protocol::subscribe_response::confirmation::Kind::ProcessId(p),
                            ),
                        },
                    )),
                },
            },
            SubscriptionEvent::EventAppeared(e) => protocol::SubscribeResponse {
                event: Some(protocol::subscribe_response::Event::EventAppeared(
                    protocol::subscribe_response::EventAppeared {
                        event: Some(e.into()),
                    },
                )),
            },
            SubscriptionEvent::CaughtUp => protocol::SubscribeResponse {
                event: Some(protocol::subscribe_response::Event::CaughtUp(
                    protocol::subscribe_response::CaughtUp {},
                )),
            },
            SubscriptionEvent::Unsubscribed(_) => protocol::SubscribeResponse {
                event: Some(protocol::subscribe_response::Event::Error(
                    protocol::subscribe_response::Error {},
                )),
            },

            SubscriptionEvent::Notification(n) => protocol::SubscribeResponse {
                event: Some(protocol::subscribe_response::Event::Notification(n.into())),
            },
        }
    }
}

impl From<protocol::subscribe_response::Error> for SubscriptionError {
    fn from(_: protocol::subscribe_response::Error) -> Self {
        SubscriptionError {}
    }
}

#[derive(Debug)]
pub enum DeleteStreamCompleted {
    Success(WriteResult),
    Error(DeleteError),
}

impl DeleteStreamCompleted {
    pub fn success(self) -> eyre::Result<WriteResult> {
        if let Self::Success(r) = self {
            return Ok(r);
        }

        eyre::bail!("stream deletion failed")
    }
}

#[derive(Debug)]
pub enum DeleteError {
    StreamDeleted,
    WrongExpectedRevision(WrongExpectedRevisionError),
    NotLeaderException(EndPoint),
}

impl DeleteError {
    pub fn is_stream_deleted(&self) -> bool {
        matches!(self, DeleteError::StreamDeleted)
    }
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
                write!(f, "not leader exception: {}:{}", e.host, e.port)
            }

            DeleteError::StreamDeleted => {
                write!(f, "stream deleted")
            }
        }
    }
}

impl From<protocol::DeleteStreamResponse> for DeleteStreamCompleted {
    fn from(value: protocol::DeleteStreamResponse) -> Self {
        match value.result.unwrap() {
            protocol::delete_stream_response::Result::WriteResult(r) => {
                DeleteStreamCompleted::Success(WriteResult {
                    next_expected_version: ExpectedRevision::Revision(r.next_revision),
                    position: r.position,
                    next_logical_position: 0,
                })
            }

            protocol::delete_stream_response::Result::Error(e) => match e.error.unwrap() {
                protocol::delete_stream_response::error::Error::WrongRevision(e) => {
                    DeleteStreamCompleted::Error(DeleteError::WrongExpectedRevision(
                        WrongExpectedRevisionError {
                            expected: e.expected_revision.unwrap().into(),
                            current: e.current_revision.unwrap().into(),
                        },
                    ))
                }

                protocol::delete_stream_response::error::Error::NotLeader(e) => {
                    DeleteStreamCompleted::Error(DeleteError::NotLeaderException(EndPoint {
                        host: e.leader_host,
                        port: e.leader_port as u16,
                    }))
                }

                protocol::delete_stream_response::error::Error::StreamDeleted(_) => {
                    DeleteStreamCompleted::Error(DeleteError::StreamDeleted)
                }
            },
        }
    }
}

impl From<DeleteStreamCompleted> for protocol::DeleteStreamResponse {
    fn from(value: DeleteStreamCompleted) -> Self {
        match value {
            DeleteStreamCompleted::Success(w) => protocol::DeleteStreamResponse {
                result: Some(protocol::delete_stream_response::Result::WriteResult(
                    w.into(),
                )),
            },

            DeleteStreamCompleted::Error(e) => protocol::DeleteStreamResponse {
                result: Some(protocol::delete_stream_response::Result::Error(
                    protocol::delete_stream_response::Error {
                        error: Some(match e {
                            DeleteError::WrongExpectedRevision(e) => {
                                protocol::delete_stream_response::error::Error::WrongRevision(
                                    e.into(),
                                )
                            }

                            DeleteError::NotLeaderException(e) => {
                                protocol::delete_stream_response::error::Error::NotLeader(
                                    protocol::delete_stream_response::error::NotLeader {
                                        leader_host: e.host,
                                        leader_port: e.port as u32,
                                    },
                                )
                            }

                            DeleteError::StreamDeleted => {
                                protocol::delete_stream_response::error::Error::StreamDeleted(())
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

impl From<ListPrograms> for protocol::ListProgramsRequest {
    fn from(_: ListPrograms) -> Self {
        Self { empty: None }
    }
}

impl From<protocol::ListProgramsRequest> for ListPrograms {
    fn from(_: protocol::ListProgramsRequest) -> Self {
        Self {}
    }
}

#[derive(Clone, Debug)]
pub struct GetProgramStats {
    pub id: u64,
}

impl From<GetProgramStats> for protocol::ProgramStatsRequest {
    fn from(value: GetProgramStats) -> Self {
        Self { id: value.id }
    }
}

impl From<protocol::ProgramStatsRequest> for GetProgramStats {
    fn from(value: protocol::ProgramStatsRequest) -> Self {
        Self { id: value.id }
    }
}

#[derive(Clone, Debug)]
pub struct KillProgram {
    pub id: u64,
}

impl From<KillProgram> for protocol::StopProgramRequest {
    fn from(value: KillProgram) -> Self {
        Self { id: value.id }
    }
}

impl From<protocol::StopProgramRequest> for KillProgram {
    fn from(value: protocol::StopProgramRequest) -> Self {
        Self { id: value.id }
    }
}

#[derive(Clone, Debug)]
pub struct ProgramListed {
    pub programs: Vec<ProgramSummary>,
}

#[derive(Clone, Debug)]
pub struct ProgramSummary {
    pub id: u64,
    pub name: String,
    pub started_at: DateTime<Utc>,
}

impl From<protocol::list_programs_response::ProgramSummary> for ProgramSummary {
    fn from(value: protocol::list_programs_response::ProgramSummary) -> Self {
        Self {
            id: value.id,
            name: value.name,
            started_at: Utc.timestamp_opt(value.started_at, 0).unwrap(),
        }
    }
}

impl From<ProgramSummary> for protocol::list_programs_response::ProgramSummary {
    fn from(value: ProgramSummary) -> Self {
        Self {
            id: value.id,
            name: value.name,
            started_at: value.started_at.timestamp(),
        }
    }
}

impl From<protocol::ListProgramsResponse> for ProgramListed {
    fn from(value: protocol::ListProgramsResponse) -> Self {
        Self {
            programs: value.programs.into_iter().map(|p| p.into()).collect(),
        }
    }
}

impl From<ProgramListed> for protocol::ListProgramsResponse {
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

impl From<protocol::StopProgramResponse> for ProgramKilled {
    fn from(value: protocol::StopProgramResponse) -> Self {
        match value.result.unwrap() {
            protocol::stop_program_response::Result::Success(_) => ProgramKilled::Success,
            protocol::stop_program_response::Result::Error(e) => match e.error.unwrap() {
                protocol::stop_program_response::error::Error::NotExists(_) => {
                    ProgramKilled::Error(ProgramKillError::NotExists)
                }
            },
        }
    }
}

impl From<ProgramKilled> for protocol::StopProgramResponse {
    fn from(value: ProgramKilled) -> Self {
        match value {
            ProgramKilled::Success => protocol::StopProgramResponse {
                result: Some(protocol::stop_program_response::Result::Success(())),
            },

            ProgramKilled::Error(e) => protocol::StopProgramResponse {
                result: Some(protocol::stop_program_response::Result::Error(
                    protocol::stop_program_response::Error {
                        error: Some(match e {
                            ProgramKillError::NotExists => {
                                protocol::stop_program_response::error::Error::NotExists(())
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
    pub id: u64,
    pub name: String,
    pub source_code: String,
    pub subscriptions: Vec<String>,
    pub pushed_events: usize,
    pub started: DateTime<Utc>,
}

impl From<protocol::program_stats_response::ProgramStats> for ProgramStats {
    fn from(value: protocol::program_stats_response::ProgramStats) -> Self {
        Self {
            id: value.id,
            name: value.name,
            source_code: value.source_code,
            subscriptions: value.subscriptions,
            pushed_events: value.pushed_events as usize,
            started: Utc.timestamp_opt(value.started_at, 0).unwrap(),
        }
    }
}

impl From<ProgramStats> for protocol::program_stats_response::ProgramStats {
    fn from(value: ProgramStats) -> Self {
        Self {
            id: value.id,
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

impl From<protocol::ProgramStatsResponse> for ProgramObtained {
    fn from(value: protocol::ProgramStatsResponse) -> Self {
        match value.result.unwrap() {
            protocol::program_stats_response::Result::Program(stats) => {
                ProgramObtained::Success(stats.into())
            }

            protocol::program_stats_response::Result::Error(e) => match e.error.unwrap() {
                protocol::program_stats_response::error::Error::NotExists(_) => {
                    ProgramObtained::Error(GetProgramError::NotExists)
                }
            },
        }
    }
}

impl From<ProgramObtained> for protocol::ProgramStatsResponse {
    fn from(value: ProgramObtained) -> Self {
        match value {
            ProgramObtained::Success(stats) => protocol::ProgramStatsResponse {
                result: Some(protocol::program_stats_response::Result::Program(
                    stats.into(),
                )),
            },

            ProgramObtained::Error(e) => protocol::ProgramStatsResponse {
                result: Some(protocol::program_stats_response::Result::Error(
                    protocol::program_stats_response::Error {
                        error: Some(match e {
                            GetProgramError::NotExists => {
                                protocol::program_stats_response::error::Error::NotExists(())
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
