use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::any::type_name;
use std::fmt::Display;
use thiserror::Error;
use uuid::Uuid;

pub use client::{SubscriptionEvent, SubscriptionNotification, UnsubscribeReason};
pub use io::{IteratorIO, IteratorIOExt};

mod client;
mod io;

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

#[derive(Clone)]
pub struct DeleteStream {
    pub stream_name: String,
    pub expected_revision: ExpectedRevision,
}

#[derive(Clone)]
pub struct ReadStream {
    pub stream_name: String,
    pub direction: Direction,
    pub revision: Revision<u64>,
    pub max_count: u64,
}

#[derive(Clone)]
pub enum Subscribe {
    ToProgram(SubscribeToProgram),
    ToStream(SubscribeToStream),
}

#[derive(Clone)]
pub struct SubscribeToProgram {
    pub name: String,
    pub source: String,
}

#[derive(Clone)]
pub struct SubscribeToStream {
    pub stream_name: String,
    pub start: Revision<u64>,
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

#[derive(Clone, Debug)]
pub struct ListPrograms {}

#[derive(Clone, Debug)]
pub struct GetProgramStats {
    pub id: u64,
}

#[derive(Clone, Debug)]
pub struct KillProgram {
    pub id: u64,
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

#[derive(Clone, Copy, Debug)]
pub enum ProgramKilled {
    Success,
    Error(ProgramKillError),
}

#[derive(Clone, Copy, Debug)]
pub enum ProgramKillError {
    NotExists,
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

#[derive(Debug)]
pub enum ProgramObtained {
    Success(ProgramStats),
    Error(GetProgramError),
}

#[derive(Debug)]
pub enum GetProgramError {
    NotExists,
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
