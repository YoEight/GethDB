use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};

pub mod internal {
    pub use crate::schema_generated::geth::*;
}

mod schema_generated;

pub type PersistCommand<'a> = WIPOffset<internal::Command<'a>>;

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
    ) -> (
        internal::StreamExpectation,
        Option<WIPOffset<UnionWIPOffset>>,
    ) {
        match self {
            ExpectedRevision::Any => (internal::StreamExpectation::NONE, None),
            ExpectedRevision::Empty => (internal::StreamExpectation::ExpectEmpty, None),
            ExpectedRevision::Exists => (internal::StreamExpectation::ExpectExists, None),
            ExpectedRevision::Revision(revision) => {
                let revision = internal::ExpectRevision::create(
                    builder,
                    &mut internal::ExpectRevisionArgs { revision },
                )
                .as_union_value();

                (internal::StreamExpectation::ExpectRevision, Some(revision))
            }
        }
    }
}

pub struct ProposedEvent<'a> {
    pub r#type: &'a str,
    pub payload: &'a [u8],
}

pub struct AppendStream<'a> {
    builder: &'a mut FlatBufferBuilder<'a>,
    args: internal::AppendStreamArgs<'a>,
    stream_name: WIPOffset<&'a str>,
}

impl<'a> AppendStream<'a> {
    pub fn new(
        builder: &'a mut FlatBufferBuilder<'a>,
        stream_name: &'a str,
        expected: ExpectedRevision,
    ) -> Self {
        let stream_name = builder.create_shared_string(stream_name);
        let (expectation_type, expectation) = expected.serialize(builder);

        let args = internal::AppendStreamArgs {
            stream: Some(stream_name),
            expectation_type,
            expectation,
            events: None,
        };

        Self {
            builder,
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
            let class = self.builder.create_string(event.r#type);
            let payload = self.builder.create_vector(event.payload);
            let event = internal::ProposedEvent::create(
                self.builder,
                &mut internal::ProposedEventArgs {
                    class: Some(class),
                    stream: Some(self.stream_name),
                    payload: Some(payload),
                },
            );

            proposed_events.push(event);
        }

        let events = self
            .builder
            .create_vector_from_iter(proposed_events.into_iter());

        self.args.events = Some(events);
        self.finish()
    }

    pub fn finish(mut self) -> &'a [u8] {
        let command = internal::AppendStream::create(self.builder, &mut self.args).as_union_value();

        let data = internal::Command::create(
            self.builder,
            &internal::CommandArgs {
                command_type: internal::Commands::AppendStream,
                command: Some(command),
            },
        );

        self.builder.finish_minimal(data);
        self.builder.finished_data()
    }
}

pub struct DeleteStream<'a> {
    builder: &'a mut FlatBufferBuilder<'a>,
    args: internal::DeleteStreamArgs<'a>,
}

impl<'a> DeleteStream<'a> {
    pub fn new(
        builder: &'a mut FlatBufferBuilder<'a>,
        stream_name: &'a str,
        expected: ExpectedRevision,
    ) -> Self {
        let stream_name = builder.create_string(stream_name);
        let (expectation_type, expectation) = expected.serialize(builder);
        Self {
            builder,
            args: internal::DeleteStreamArgs {
                stream: Some(stream_name),
                expectation_type,
                expectation,
            },
        }
    }

    pub fn finish(mut self) -> &'a [u8] {
        let command = internal::DeleteStream::create(self.builder, &mut self.args).as_union_value();

        let data = internal::Command::create(
            self.builder,
            &internal::CommandArgs {
                command_type: internal::Commands::DeleteStream,
                command: Some(command),
            },
        );

        self.builder.finish_minimal(data);
        self.builder.finished_data()
    }
}

#[test]
fn test_serde_expectation() {
    let mut builder = FlatBufferBuilder::with_capacity(1_024);
    let (expectation_type, expectation) = ExpectedRevision::Any.serialize(&mut builder);

    assert_eq!(internal::StreamExpectation::NONE, expectation_type);
    assert!(expectation.is_none());

    let (expectation_type, expectation) = ExpectedRevision::Exists.serialize(&mut builder);
    assert_eq!(internal::StreamExpectation::ExpectExists, expectation_type);
    assert!(expectation.is_none());

    let (expectation_type, expectation) = ExpectedRevision::Empty.serialize(&mut builder);
    assert_eq!(internal::StreamExpectation::ExpectEmpty, expectation_type);
    assert!(expectation.is_none());

    let (expectation_type, expectation) = ExpectedRevision::Revision(42).serialize(&mut builder);
    assert_eq!(
        internal::StreamExpectation::ExpectRevision,
        expectation_type
    );

    assert!(expectation.is_some());

    // Right now, I don't see how to deserialize the expectation to a `ExpectRevision` table. We are
    // not supposed to consume stream expectation in such fashion as each buffer uses them provide
    // method to parse them.
}

#[test]
fn test_serde_append_stream() {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

    let data = AppendStream::new(&mut builder, "foobar", ExpectedRevision::Revision(42))
        .with_event(ProposedEvent {
            r#type: "user-created",
            payload: b"qwerty",
        });

    let actual = flatbuffers::root::<internal::Command>(data).unwrap();
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
    let mut builder = FlatBufferBuilder::with_capacity(1_024);

    let data = DeleteStream::new(&mut builder, "foobar", ExpectedRevision::Revision(42)).finish();
    let actual = flatbuffers::root::<internal::Command>(data).unwrap();
    let actual_delete = actual.command_as_delete_stream().unwrap();
    let stream_name = actual_delete.stream().unwrap();
    let expectation = actual_delete.expectation_as_expect_revision().unwrap();

    assert_eq!("foobar", stream_name);
    assert_eq!(42, expectation.revision());
}
