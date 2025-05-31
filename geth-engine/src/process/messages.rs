use geth_common::{Direction, ExpectedRevision, ProgramStats, ProgramSummary, Propose, Record};
use geth_domain::index::BlockEntry;
use geth_mikoshi::wal::LogEntry;
use tokio::sync::mpsc::UnboundedSender;

use crate::domain::index::CurrentRevision;

use super::ProcId;

#[derive(Debug)]
pub enum Messages {
    Requests(Requests),
    Responses(Responses),
}

#[cfg(test)]
impl Messages {
    pub fn is_fatal_error(&self) -> bool {
        matches!(self, Messages::Responses(Responses::FatalError))
    }
}

impl From<IndexRequests> for Messages {
    fn from(req: IndexRequests) -> Self {
        Messages::Requests(Requests::Index(req))
    }
}

impl From<IndexResponses> for Messages {
    fn from(resp: IndexResponses) -> Self {
        Messages::Responses(Responses::Index(resp))
    }
}

impl From<ReadResponses> for Messages {
    fn from(resp: ReadResponses) -> Self {
        Messages::Responses(Responses::Read(resp))
    }
}

impl From<ReadRequests> for Messages {
    fn from(req: ReadRequests) -> Self {
        Messages::Requests(Requests::Read(req))
    }
}

impl From<SubscribeRequests> for Messages {
    fn from(req: SubscribeRequests) -> Self {
        Messages::Requests(Requests::Subscribe(req))
    }
}

impl From<SubscribeResponses> for Messages {
    fn from(resp: SubscribeResponses) -> Self {
        Messages::Responses(Responses::Subscribe(resp))
    }
}

impl From<WriteRequests> for Messages {
    fn from(req: WriteRequests) -> Self {
        Messages::Requests(Requests::Write(req))
    }
}

impl From<WriteResponses> for Messages {
    fn from(resp: WriteResponses) -> Self {
        Messages::Responses(Responses::Write(resp))
    }
}

impl From<ProgramRequests> for Messages {
    fn from(req: ProgramRequests) -> Self {
        Messages::Requests(Requests::Program(req))
    }
}

impl From<ProgramResponses> for Messages {
    fn from(resp: ProgramResponses) -> Self {
        Messages::Responses(Responses::Program(resp))
    }
}

impl From<TestSinkRequests> for Messages {
    fn from(req: TestSinkRequests) -> Self {
        Messages::Requests(Requests::TestSink(req))
    }
}

impl From<TestSinkResponses> for Messages {
    fn from(resp: TestSinkResponses) -> Self {
        Messages::Responses(Responses::TestSink(resp))
    }
}

impl From<RequestError> for Messages {
    fn from(err: RequestError) -> Self {
        Messages::Responses(Responses::Error(err))
    }
}

impl TryFrom<Messages> for IndexResponses {
    type Error = ();

    fn try_from(msg: Messages) -> Result<Self, ()> {
        match msg {
            Messages::Responses(Responses::Index(resp)) => Ok(resp),
            _ => Err(()),
        }
    }
}

impl TryFrom<Messages> for IndexRequests {
    type Error = ();

    fn try_from(msg: Messages) -> Result<Self, ()> {
        match msg {
            Messages::Requests(Requests::Index(req)) => Ok(req),
            _ => Err(()),
        }
    }
}

impl TryFrom<Messages> for ReadRequests {
    type Error = ();

    fn try_from(msg: Messages) -> Result<Self, ()> {
        match msg {
            Messages::Requests(Requests::Read(req)) => Ok(req),
            _ => Err(()),
        }
    }
}

impl TryFrom<Messages> for ReadResponses {
    type Error = ();

    fn try_from(msg: Messages) -> Result<Self, ()> {
        match msg {
            Messages::Responses(Responses::Read(resp)) => Ok(resp),
            _ => Err(()),
        }
    }
}

impl TryFrom<Messages> for SubscribeRequests {
    type Error = ();

    fn try_from(msg: Messages) -> Result<Self, ()> {
        match msg {
            Messages::Requests(Requests::Subscribe(req)) => Ok(req),
            _ => Err(()),
        }
    }
}

impl TryFrom<Messages> for SubscribeResponses {
    type Error = ();

    fn try_from(msg: Messages) -> Result<Self, ()> {
        match msg {
            Messages::Responses(Responses::Subscribe(resp)) => Ok(resp),
            _ => Err(()),
        }
    }
}

impl TryFrom<Messages> for WriteRequests {
    type Error = ();

    fn try_from(msg: Messages) -> Result<Self, ()> {
        match msg {
            Messages::Requests(Requests::Write(req)) => Ok(req),
            _ => Err(()),
        }
    }
}

impl TryFrom<Messages> for WriteResponses {
    type Error = ();

    fn try_from(msg: Messages) -> Result<Self, ()> {
        match msg {
            Messages::Responses(Responses::Write(resp)) => Ok(resp),
            _ => Err(()),
        }
    }
}

impl TryFrom<Messages> for ProgramRequests {
    type Error = ();

    fn try_from(msg: Messages) -> Result<Self, ()> {
        match msg {
            Messages::Requests(Requests::Program(req)) => Ok(req),
            _ => Err(()),
        }
    }
}

impl TryFrom<Messages> for TestSinkRequests {
    type Error = ();

    fn try_from(msg: Messages) -> Result<Self, ()> {
        match msg {
            Messages::Requests(Requests::TestSink(req)) => Ok(req),
            _ => Err(()),
        }
    }
}

impl TryFrom<Messages> for TestSinkResponses {
    type Error = ();

    fn try_from(msg: Messages) -> Result<Self, ()> {
        match msg {
            Messages::Responses(Responses::TestSink(resp)) => Ok(resp),
            _ => Err(()),
        }
    }
}

impl TryFrom<Messages> for ProgramResponses {
    type Error = ();

    fn try_from(msg: Messages) -> Result<Self, ()> {
        match msg {
            Messages::Responses(Responses::Program(resp)) => Ok(resp),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
pub enum Requests {
    Index(IndexRequests),
    Read(ReadRequests),
    Subscribe(SubscribeRequests),
    Write(WriteRequests),
    Program(ProgramRequests),
    TestSink(TestSinkRequests),
}

#[derive(Debug)]
pub enum IndexRequests {
    Read {
        key: u64,
        start: u64,
        count: usize,
        dir: Direction,
    },

    Store {
        entries: Vec<BlockEntry>,
    },

    LatestRevision {
        key: u64,
    },
}

#[derive(Debug)]
pub enum ReadRequests {
    Read {
        ident: String,
        start: u64,
        direction: Direction,
        count: usize,
    },

    ReadAt {
        position: u64,
    },
}

#[derive(Debug)]
pub enum SubscribeRequests {
    Subscribe(SubscriptionType),
    Program(ProgramRequests),
    Push { events: Vec<Record> },
}

#[derive(Debug)]
pub enum SubscriptionType {
    Stream { ident: String },
    Program { name: String, code: String },
}

#[derive(Debug)]
pub enum WriteRequests {
    Write {
        ident: String,
        expected: ExpectedRevision,
        events: Vec<Propose>,
    },

    Delete {
        ident: String,
        expected: ExpectedRevision,
    },
}

#[derive(Debug)]
pub enum ProgramRequests {
    Start {
        name: String,
        code: String,
        sender: UnboundedSender<Messages>,
    },

    Stats {
        id: ProcId,
    },

    List,

    Stop {
        id: ProcId,
    },
}

#[derive(Debug)]
pub enum TestSinkRequests {
    StreamFrom { low: u64, high: u64 },
}

#[derive(Debug)]
pub enum Responses {
    Index(IndexResponses),
    Read(ReadResponses),
    Subscribe(SubscribeResponses),
    Write(WriteResponses),
    Program(ProgramResponses),
    TestSink(TestSinkResponses),
    Error(RequestError),
    FatalError,
}

#[derive(Debug)]
pub enum RequestError {
    NotFound,
}

#[derive(Debug)]
pub enum IndexResponses {
    Error,
    StreamDeleted,
    Entries(Vec<BlockEntry>),
    CurrentRevision(CurrentRevision),
    Committed,
}

#[derive(Debug)]
pub enum ReadResponses {
    Error,
    StreamDeleted,
    Entries(Vec<LogEntry>),
    Entry(LogEntry),
}

#[derive(Debug)]
pub enum SubscribeResponses {
    Error(eyre::Report),
    Programs(ProgramResponses),
    Confirmed,
    Record(Record),
}

#[derive(Debug)]
pub enum WriteResponses {
    Error,
    StreamDeleted,

    WrongExpectedRevision {
        expected: ExpectedRevision,
        current: ExpectedRevision,
    },

    Committed {
        start_position: u64,
        next_position: u64,
        next_expected_version: ExpectedRevision,
    },

    WritePosition(u64),
}

#[derive(Debug)]
pub enum TestSinkResponses {
    Stream(u64),
}

#[derive(Debug)]
pub enum ProgramResponses {
    Stats(ProgramStats),
    Summary(ProgramSummary),
    List(Vec<ProgramSummary>),
    NotFound,
    Error(eyre::Report),
    Started,
    Stopped,
}
