use geth_common::{Direction, ExpectedRevision, Propose};
use geth_domain::index::BlockEntry;
use geth_mikoshi::wal::LogEntry;

use crate::domain::index::CurrentRevision;

pub enum Messages {
    Requests(Requests),
    Responses(Responses),
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

pub enum Requests {
    Index(IndexRequests),
    Read(ReadRequests),
    Subscribe(SubscribeRequests),
    Write(WriteRequests),
    TestSink(TestSinkRequests),
}

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

pub enum SubscribeRequests {
    Subscribe { ident: String },
    Push { events: Vec<LogEntry> },
}

pub enum WriteRequests {
    Write {
        ident: String,
        expected: ExpectedRevision,
        events: Vec<Propose>,
    },
}

pub enum TestSinkRequests {
    StreamFrom { low: u64, high: u64 },
}

pub enum Responses {
    Index(IndexResponses),
    Read(ReadResponses),
    Subscribe(SubscribeResponses),
    Write(WriteResponses),
    TestSink(TestSinkResponses),
}

pub enum IndexResponses {
    Error,
    StreamDeleted,
    Entries(Vec<BlockEntry>),
    CurrentRevision(CurrentRevision),
    Committed,
}

pub enum ReadResponses {
    Error,
    StreamDeleted,
    Entries(Vec<LogEntry>),
    Entry(LogEntry),
}

pub enum SubscribeResponses {
    Error,
    Confirmed,
    Entry(LogEntry),
}

pub enum WriteResponses {
    Error,
    StreamDeleted,

    WrongExpectedRevision {
        expected: ExpectedRevision,
        current: ExpectedRevision,
    },

    Committed {
        start: u64,
        next: u64,
    },
}

pub enum TestSinkResponses {
    Stream(u64),
}
