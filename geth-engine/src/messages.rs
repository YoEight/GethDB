use eyre::Report;
use uuid::Uuid;

use crate::process::reading;
use geth_common::{Direction, ExpectedRevision, Propose, Revision};
use geth_mikoshi::MikoshiStream;

#[derive(Debug)]
pub struct ReadStream {
    pub correlation: Uuid,
    pub stream_name: String,
    pub direction: Direction,
    pub starting: Revision<u64>,
    pub count: usize,
}

pub enum ReadStreamCompleted {
    StreamDeleted,
    Unexpected(Report),
    Success(reading::Streaming),
}

impl ReadStreamCompleted {
    pub fn success(self) -> eyre::Result<reading::Streaming> {
        if let ReadStreamCompleted::Success(stream) = self {
            return Ok(stream);
        }

        eyre::bail!("read stream failed")
    }
}

#[derive(Debug)]
pub struct AppendStream {
    pub stream_name: String,
    pub events: Vec<Propose>,
    pub expected: ExpectedRevision,
}

#[derive(Debug)]
pub struct DeleteStream {
    pub stream_name: String,
    pub expected: ExpectedRevision,
}

#[derive(Debug)]
pub struct SubscribeTo {
    pub target: SubscriptionTarget,
}

pub struct SubscriptionConfirmed {
    pub outcome: SubscriptionRequestOutcome,
}

pub enum SubscriptionRequestOutcome {
    Success(MikoshiStream),
    Failure(Report),
}

#[derive(Debug)]
pub enum SubscriptionTarget {
    Stream(StreamTarget),
    Process(ProcessTarget),
}

#[derive(Debug)]
pub struct StreamTarget {
    pub parent: Option<Uuid>,
    pub stream_name: String,
}

#[derive(Debug)]
pub struct ProcessTarget {
    pub id: Uuid,
    pub name: String,
    pub source_code: String,
}
