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

    pub fn finish(mut self) -> PersistCommand<'a> {
        let command = internal::AppendStream::create(self.builder, &mut self.args).as_union_value();

        internal::Command::create(
            self.builder,
            &internal::CommandArgs {
                command_type: internal::Commands::AppendStream,
                command: Some(command),
            },
        )
    }
}

#[test]
fn test_serde() {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
    let stream_name = builder.create_shared_string("foobar");

    let mut events = Vec::new();
    let class = builder.create_string("user-created");

    events.push(internal::ProposedEvent::create(
        &mut builder,
        &mut internal::ProposedEventArgs {
            class: Some(class),
            stream: Some(stream_name),
            revision: 42,
            payload: None,
        },
    ));

    let events = builder.create_vector(events.as_slice());

    let expect = internal::ExpectRevision::create(
        &mut builder,
        &mut internal::ExpectRevisionArgs { revision: 41 },
    )
    .as_union_value();

    let append_stream = internal::AppendStream::create(
        &mut builder,
        &mut internal::AppendStreamArgs {
            stream: Some(stream_name),
            expectation_type: internal::StreamExpectation::ExpectRevision,
            expectation: Some(expect),
            events: Some(events),
        },
    );

    let command = internal::Command::create(
        &mut builder,
        &mut internal::CommandArgs {
            command_type: internal::Commands::AppendStream,
            command: Some(append_stream.as_union_value()),
        },
    );

    builder.finish_minimal(command);
    let data = builder.finished_data();

    let actual = flatbuffers::root::<internal::Command>(data).unwrap();

    let toto = actual.command_type();
    let actual_app = actual.command_as_append_stream().unwrap();

    let my_stream = actual_app.stream().unwrap_or_default();
    println!("toto");
}
