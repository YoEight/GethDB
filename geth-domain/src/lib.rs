use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use prost::Message;
use uuid::Uuid;

use crate::binary::events::Events;

pub mod binary;
mod iter;

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum ExpectedRevision {
    Any,
    Empty,
    Exists,
    Revision(u64),
}

pub struct ProposedEvent<'a> {
    pub r#type: &'a str,
    pub payload: &'a [u8],
}

pub struct RecordedEvent {
    pub id: Uuid,
    pub revision: u64,
    pub stream_name: String,
    pub class: String,
    pub created: DateTime<Utc>,
    pub data: Bytes,
    pub metadata: Bytes,
}

impl RecordedEvent {
    pub fn from(inner: binary::events::RecordedEvent) -> RecordedEvent {
        Self {
            id: inner.id.into(),
            revision: inner.revision,
            stream_name: inner.stream_name,
            class: inner.class,
            created: Utc.timestamp_opt(inner.created, 0).unwrap(),
            data: inner.data,
            metadata: inner.metadata,
        }
    }
}

pub fn parse_event(payload: &[u8]) -> eyre::Result<Events> {
    let evt = Events::decode(payload)?;
    Ok(evt)
}

#[test]
fn test_serde_expectation() {
    let mut builder = FlatBufferBuilder::with_capacity(1_024);
    let (expectation_type, expectation) = ExpectedRevision::Any.serialize(&mut builder);

    assert_eq!(binary::StreamExpectation::NONE, expectation_type);
    assert!(expectation.is_none());

    let (expectation_type, expectation) = ExpectedRevision::Exists.serialize(&mut builder);
    assert_eq!(binary::StreamExpectation::ExpectExists, expectation_type);
    assert!(expectation.is_none());

    let (expectation_type, expectation) = ExpectedRevision::Empty.serialize(&mut builder);
    assert_eq!(binary::StreamExpectation::ExpectEmpty, expectation_type);
    assert!(expectation.is_none());

    let (expectation_type, expectation) = ExpectedRevision::Revision(42).serialize(&mut builder);
    assert_eq!(binary::StreamExpectation::ExpectRevision, expectation_type);

    assert!(expectation.is_some());

    // Right now, I don't see how to deserialize the expectation to a `ExpectRevision` table. We are
    // not supposed to consume stream expectation in such fashion as each buffer uses them provide
    // method to parse them.
}

#[test]
fn test_serde_append_stream() {
    let mut domain = Domain::new();

    let data = domain
        .commands()
        .append_stream("foobar", ExpectedRevision::Revision(42))
        .with_event(ProposedEvent {
            r#type: "user-created",
            payload: b"qwerty",
        });

    let actual = flatbuffers::root::<binary::Command>(data).unwrap();
    let actual_app = actual.command_as_append_stream().unwrap();
    let stream_name = actual_app.stream().unwrap_or_default();
    let events = actual_app.events().unwrap();
    let event = events.get(0);
    let expectation = actual_app.expectation_as_expect_revision().unwrap();

    assert_eq!("foobar", stream_name);
    assert_eq!(42, expectation.revision());
    assert_eq!("user-created", event.class().unwrap());
    assert_eq!(b"qwerty", event.payload().unwrap().bytes());
}

#[test]
fn test_serde_delete_stream() {
    let mut domain = Domain::new();

    let data = domain
        .commands()
        .delete_stream("foobar", ExpectedRevision::Revision(42))
        .complete();

    let actual = flatbuffers::root::<binary::Command>(data).unwrap();
    let actual_delete = actual.command_as_delete_stream().unwrap();
    let stream_name = actual_delete.stream().unwrap();
    let expectation = actual_delete.expectation_as_expect_revision().unwrap();

    assert_eq!("foobar", stream_name);
    assert_eq!(42, expectation.revision());
}
