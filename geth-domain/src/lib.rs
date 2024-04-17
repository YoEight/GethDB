use std::sync::{Arc, LockResult, Mutex, MutexGuard};

use chrono::Utc;
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use uuid::Uuid;

pub mod binary {
    pub use crate::commands_generated::geth::*;
    pub use crate::events_generated::geth::*;
}

mod commands_generated;
mod events_generated;
mod iter;
mod tf_log;

pub type PersistCommand<'a> = WIPOffset<binary::Command<'a>>;

#[derive(Clone)]
pub struct DomainRef {
    pub inner: Arc<Mutex<Domain>>,
}

impl DomainRef {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Domain::new())),
        }
    }

    pub fn lock(&self) -> LockResult<MutexGuard<'_, Domain>> {
        self.inner.lock()
    }
}

pub struct Domain {
    builder: FlatBufferBuilder<'static>,
}

impl Domain {
    pub fn new() -> Self {
        Self {
            builder: FlatBufferBuilder::with_capacity(4_096),
        }
    }

    pub fn create_shared_string<'a: 'b, 'b>(
        &'a mut self,
        string: &'b str,
    ) -> WIPOffset<&'static str> {
        self.builder.create_shared_string(string)
    }

    pub fn create_string<'a: 'b, 'b>(&'a mut self, string: &'b str) -> WIPOffset<&'static str> {
        self.builder.create_string(string)
    }

    pub fn commands<'a: 'b, 'b>(&'a mut self) -> CommandBuilder<'b> {
        CommandBuilder { inner: self }
    }

    pub fn events<'a: 'b, 'b>(&'a mut self) -> EventBuilder<'b> {
        EventBuilder { inner: self }
    }
}

pub struct CommandBuilder<'a> {
    inner: &'a mut Domain,
}

impl<'a> CommandBuilder<'a> {
    pub fn append_stream(
        self,
        stream_name: &'a str,
        expected: ExpectedRevision,
    ) -> AppendStream<'a> {
        AppendStream::new(self.inner, stream_name, expected)
    }

    pub fn delete_stream(
        self,
        stream_name: &'a str,
        expected: ExpectedRevision,
    ) -> DeleteStream<'a> {
        DeleteStream::new(self.inner, stream_name, expected)
    }
}

pub struct EventBuilder<'a> {
    inner: &'a mut Domain,
}

impl<'a> EventBuilder<'a> {
    pub fn recorded_event(
        mut self,
        id: Uuid,
        stream_name: WIPOffset<&'a str>,
        class: WIPOffset<&'a str>,
        revision: u64,
    ) -> &'a [u8] {
        let created = Utc::now().timestamp();
        let (most, least) = id.as_u64_pair();
        let mut id = binary::Id::default();
        id.set_high(most);
        id.set_low(least);

        let evt = binary::RecordedEvent::create(
            &mut self.inner.builder,
            &mut binary::RecordedEventArgs {
                id: Some(&id),
                revision,
                stream_name: Some(stream_name),
                class: Some(class),
                created,
                data: None,
                metadata: None,
            },
        )
        .as_union_value();

        let evt = binary::Event::create(
            &mut self.inner.builder,
            &mut binary::EventArgs {
                event_type: binary::Events::RecordedEvent,
                event: Some(evt),
            },
        );

        self.inner.builder.finish_minimal(evt);
        self.inner.builder.finished_data()
    }

    pub fn stream_deleted(mut self, stream_name: &'a str, revision: u64) -> &'a [u8] {
        let created = Utc::now().timestamp();
        let stream_name = self.inner.builder.create_string(stream_name);
        let evt = binary::StreamDeleted::create(
            &mut self.inner.builder,
            &mut binary::StreamDeletedArgs {
                stream_name: Some(stream_name),
                revision,
                created,
            },
        )
        .as_union_value();

        let evt = binary::Event::create(
            &mut self.inner.builder,
            &mut binary::EventArgs {
                event_type: binary::Events::StreamDeleted,
                event: Some(evt),
            },
        );

        self.inner.builder.finish_minimal(evt);
        self.inner.builder.finished_data()
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum ExpectedRevision {
    Any,
    Empty,
    Exists,
    Revision(u64),
}

impl ExpectedRevision {
    pub fn serialize(
        self,
        builder: &mut FlatBufferBuilder,
    ) -> (binary::StreamExpectation, Option<WIPOffset<UnionWIPOffset>>) {
        match self {
            ExpectedRevision::Any => (binary::StreamExpectation::NONE, None),
            ExpectedRevision::Empty => (binary::StreamExpectation::ExpectEmpty, None),
            ExpectedRevision::Exists => (binary::StreamExpectation::ExpectExists, None),
            ExpectedRevision::Revision(revision) => {
                let revision = binary::ExpectRevision::create(
                    builder,
                    &mut binary::ExpectRevisionArgs { revision },
                )
                .as_union_value();

                (binary::StreamExpectation::ExpectRevision, Some(revision))
            }
        }
    }
}

pub fn parse_command(bytes: &[u8]) -> eyre::Result<binary::Command> {
    let cmd = flatbuffers::root::<binary::Command>(bytes)?;

    Ok(cmd)
}

pub fn parse_event(bytes: &[u8]) -> eyre::Result<binary::Event> {
    let event = flatbuffers::root::<binary::Event>(bytes)?;

    Ok(event)
}

pub struct ProposedEvent<'a> {
    pub r#type: &'a str,
    pub payload: &'a [u8],
}

pub struct AppendStream<'a> {
    domain: &'a mut Domain,
    args: binary::AppendStreamArgs<'a>,
    stream_name: WIPOffset<&'a str>,
}

impl<'a> AppendStream<'a> {
    pub fn new(domain: &'a mut Domain, stream_name: &'a str, expected: ExpectedRevision) -> Self {
        let stream_name = domain.builder.create_shared_string(stream_name);
        let (expectation_type, expectation) = expected.serialize(&mut domain.builder);

        let args = binary::AppendStreamArgs {
            stream: Some(stream_name),
            expectation_type,
            expectation,
            events: None,
        };

        Self {
            domain,
            args,
            stream_name,
        }
    }

    pub fn with_event(self, event: ProposedEvent) -> &'a [u8] {
        self.with_events(&[event])
    }

    pub fn with_events(mut self, events: &[ProposedEvent]) -> &'a [u8] {
        let mut proposed_events = Vec::new();

        for event in events {
            let class = self.domain.builder.create_string(event.r#type);
            let payload = self.domain.builder.create_vector(event.payload);
            let event = binary::ProposedEvent::create(
                &mut self.domain.builder,
                &mut binary::ProposedEventArgs {
                    class: Some(class),
                    stream: Some(self.stream_name),
                    payload: Some(payload),
                },
            );

            proposed_events.push(event);
        }

        let events = self
            .domain
            .builder
            .create_vector_from_iter(proposed_events.into_iter());

        self.args.events = Some(events);
        self.finish()
    }

    pub fn finish(mut self) -> &'a [u8] {
        let command =
            binary::AppendStream::create(&mut self.domain.builder, &mut self.args).as_union_value();

        let data = binary::Command::create(
            &mut self.domain.builder,
            &binary::CommandArgs {
                command_type: binary::Commands::AppendStream,
                command: Some(command),
            },
        );

        self.domain.builder.finish_minimal(data);
        self.domain.builder.finished_data()
    }
}

pub struct DeleteStream<'a> {
    domain: &'a mut Domain,
    args: binary::DeleteStreamArgs<'a>,
}

impl<'a> DeleteStream<'a> {
    pub fn new(domain: &'a mut Domain, stream_name: &'a str, expected: ExpectedRevision) -> Self {
        let stream_name = domain.builder.create_string(stream_name);
        let (expectation_type, expectation) = expected.serialize(&mut domain.builder);
        Self {
            domain,
            args: binary::DeleteStreamArgs {
                stream: Some(stream_name),
                expectation_type,
                expectation,
            },
        }
    }

    pub fn complete(mut self) -> &'a [u8] {
        let command =
            binary::DeleteStream::create(&mut self.domain.builder, &mut self.args).as_union_value();

        let data = binary::Command::create(
            &mut self.domain.builder,
            &binary::CommandArgs {
                command_type: binary::Commands::DeleteStream,
                command: Some(command),
            },
        );

        self.domain.builder.finish_minimal(data);
        self.domain.builder.finished_data()
    }
}

pub struct RecordedEvent {
    pub id: Uuid,
    pub revision: u64,
    pub stream_name: String,
    pub class: String,
    pub created: i64,
    pub data: Vec<u8>,
    pub metadata: Vec<u8>,
}

impl RecordedEvent {
    pub fn from(inner: binary::RecordedEvent) -> RecordedEvent {
        let id = inner.id().unwrap();
        Self {
            id: Uuid::from_bytes(id.0),
            revision: inner.revision(),
            stream_name: inner.stream_name().unwrap_or_default().to_string(),
            class: inner.class().unwrap_or_default().to_string(),
            created: inner.created(),
            data: inner.data().unwrap_or_default().bytes().to_owned(),
            metadata: inner.metadata().unwrap_or_default().bytes().to_owned(),
        }
    }
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
