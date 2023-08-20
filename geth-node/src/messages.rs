use eyre::Report;
use geth_common::{
    Direction, ExpectedRevision, Propose, Revision, WriteResult, WrongExpectedRevisionError,
};
use geth_mikoshi::MikoshiStream;
use uuid::Uuid;

#[derive(Debug)]
pub struct ReadStream {
    pub correlation: Uuid,
    pub stream_name: String,
    pub direction: Direction,
    pub starting: Revision<u64>,
    pub count: usize,
}

pub struct ReadStreamCompleted {
    pub correlation: Uuid,
    pub reader: MikoshiStream,
}

#[derive(Debug)]
pub struct AppendStream {
    pub correlation: Uuid,
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
pub enum AppendStreamCompleted {
    Success(WriteResult),
    Failure(WrongExpectedRevisionError),
}

#[derive(Debug)]
pub enum DeleteStreamCompleted {
    Success(WriteResult),
    Failure(WrongExpectedRevisionError),
}

#[derive(Debug)]
pub struct SubscribeTo {
    pub correlation: Uuid,
    pub target: SubscriptionTarget,
}

pub struct SubscriptionConfirmed {
    pub correlation: Uuid,
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
    pub starting: Revision<u64>,
}

#[derive(Debug)]
pub struct ProcessTarget {
    pub id: Uuid,
    pub name: String,
    pub source_code: String,
}
