use flatbuffers::{FlatBufferBuilder, WIPOffset};

pub mod internal {
    pub use crate::schema_generated::geth::*;
}

mod schema_generated;

pub type PersistCommand<'a> = WIPOffset<internal::Command<'a>>;

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

        let (expectation_type, expectation) = match expected {
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
        };

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

#[test]
fn test_serde() {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

    let data = AppendStream::new(&mut builder, "foobar", ExpectedRevision::Any).with_event(
        ProposedEvent {
            r#type: "user-created",
            payload: b"qwerty",
        },
    );

    let actual = flatbuffers::root::<internal::Command>(data).unwrap();

    let toto = actual.command_type();
    let actual_app = actual.command_as_append_stream().unwrap();

    let my_stream = actual_app.stream().unwrap_or_default();
    println!("toto");
}
