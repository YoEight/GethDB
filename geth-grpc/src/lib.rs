pub use crate::generated::protocol;
use chrono::{TimeZone, Utc};
use geth_common::{
    AppendError, AppendStream, AppendStreamCompleted, ContentType, DeleteError, DeleteStream,
    DeleteStreamCompleted, Direction, EndPoint, ExpectedRevision, GetProgramError, GetProgramStats,
    KillProgram, ListPrograms, ProgramKillError, ProgramKilled, ProgramListed, ProgramObtained,
    ProgramStats, ProgramSummary, Propose, ReadError, ReadStream, ReadStreamResponse, Record,
    Revision, Subscribe, SubscribeToProgram, SubscribeToStream, SubscriptionConfirmation,
    SubscriptionEvent, SubscriptionNotification, UnsubscribeReason, WriteResult,
    WrongExpectedRevisionError,
};
use uuid::Uuid;

pub mod generated {
    pub mod protocol {
        include!(concat!(env!("OUT_DIR"), "/geth.rs"));
    }
}

impl From<Direction> for protocol::read_stream_request::Direction {
    fn from(value: Direction) -> Self {
        match value {
            Direction::Forward => protocol::read_stream_request::Direction::Forwards(()),
            Direction::Backward => protocol::read_stream_request::Direction::Backwards(()),
        }
    }
}

impl From<protocol::read_stream_request::Direction> for Direction {
    fn from(value: protocol::read_stream_request::Direction) -> Self {
        match value {
            protocol::read_stream_request::Direction::Forwards(_) => Direction::Forward,
            protocol::read_stream_request::Direction::Backwards(_) => Direction::Backward,
        }
    }
}

impl From<Uuid> for protocol::Ident {
    fn from(value: Uuid) -> Self {
        let (most, least) = value.as_u64_pair();
        Self { most, least }
    }
}

impl From<protocol::Ident> for Uuid {
    fn from(value: protocol::Ident) -> Self {
        Uuid::from_u64_pair(value.most, value.least)
    }
}

impl From<AppendStream> for protocol::AppendStreamRequest {
    fn from(value: AppendStream) -> Self {
        Self {
            stream_name: value.stream_name,
            events: value.events.into_iter().map(|p| p.into()).collect(),
            expected_revision: Some(value.expected_revision.into()),
        }
    }
}

impl TryFrom<protocol::AppendStreamRequest> for AppendStream {
    type Error = tonic::Status;
    fn try_from(value: protocol::AppendStreamRequest) -> Result<Self, Self::Error> {
        let expected_revision = if let Some(e) = value.expected_revision.map(Into::into) {
            e
        } else {
            return Err(tonic::Status::invalid_argument(
                "expected revision is not provided",
            ));
        };

        let mut events = Vec::with_capacity(value.events.len());

        for event in value.events {
            events.push(event.try_into()?);
        }

        Ok(Self {
            stream_name: value.stream_name,
            events,
            expected_revision,
        })
    }
}

impl From<DeleteStream> for protocol::DeleteStreamRequest {
    fn from(value: DeleteStream) -> Self {
        Self {
            stream_name: value.stream_name,
            expected_revision: Some(value.expected_revision.into()),
        }
    }
}

impl TryFrom<protocol::DeleteStreamRequest> for DeleteStream {
    type Error = tonic::Status;

    fn try_from(value: protocol::DeleteStreamRequest) -> Result<Self, Self::Error> {
        let expected_revision = value
            .expected_revision
            .map(Into::into)
            .ok_or_else(|| tonic::Status::invalid_argument("expected_revision is missing"))?;

        Ok(Self {
            stream_name: value.stream_name,
            expected_revision,
        })
    }
}

impl From<ReadStream> for protocol::ReadStreamRequest {
    fn from(value: ReadStream) -> Self {
        Self {
            stream_name: value.stream_name,
            max_count: value.max_count,
            direction: Some(value.direction.into()),
            start: Some(value.revision.into()),
        }
    }
}

impl TryFrom<protocol::ReadStreamRequest> for ReadStream {
    type Error = tonic::Status;

    fn try_from(value: protocol::ReadStreamRequest) -> Result<Self, Self::Error> {
        let direction = if let Some(d) = value.direction.map(Into::into) {
            d
        } else {
            return Err(tonic::Status::invalid_argument("direction is missing"));
        };

        let revision = if let Some(s) = value.start.map(Into::into) {
            s
        } else {
            return Err(tonic::Status::invalid_argument("start is missing"));
        };

        Ok(Self {
            stream_name: value.stream_name,
            direction,
            revision,
            max_count: value.max_count,
        })
    }
}

impl From<Subscribe> for protocol::SubscribeRequest {
    fn from(value: Subscribe) -> Self {
        match value {
            Subscribe::ToProgram(v) => protocol::SubscribeRequest {
                to: Some(protocol::subscribe_request::To::Program(v.into())),
            },

            Subscribe::ToStream(v) => protocol::SubscribeRequest {
                to: Some(protocol::subscribe_request::To::Stream(v.into())),
            },
        }
    }
}

impl TryFrom<protocol::SubscribeRequest> for Subscribe {
    type Error = tonic::Status;

    fn try_from(value: protocol::SubscribeRequest) -> Result<Self, Self::Error> {
        let value = value
            .to
            .ok_or_else(|| tonic::Status::invalid_argument("to is missing"))?;

        match value {
            protocol::subscribe_request::To::Program(v) => Ok(Subscribe::ToProgram(v.into())),
            protocol::subscribe_request::To::Stream(v) => Ok(Subscribe::ToStream(v.try_into()?)),
        }
    }
}

impl From<Revision<u64>> for protocol::read_stream_request::Start {
    fn from(value: Revision<u64>) -> Self {
        match value {
            Revision::Start => protocol::read_stream_request::Start::Beginning(()),
            Revision::End => protocol::read_stream_request::Start::End(()),
            Revision::Revision(r) => protocol::read_stream_request::Start::Revision(r),
        }
    }
}

impl From<protocol::read_stream_request::Start> for Revision<u64> {
    fn from(value: protocol::read_stream_request::Start) -> Self {
        match value {
            protocol::read_stream_request::Start::Beginning(_) => Revision::Start,
            protocol::read_stream_request::Start::End(_) => Revision::End,
            protocol::read_stream_request::Start::Revision(r) => Revision::Revision(r),
        }
    }
}

impl From<protocol::subscribe_request::stream::Start> for Revision<u64> {
    fn from(value: protocol::subscribe_request::stream::Start) -> Self {
        match value {
            protocol::subscribe_request::stream::Start::Beginning(_) => Revision::Start,
            protocol::subscribe_request::stream::Start::End(_) => Revision::End,
            protocol::subscribe_request::stream::Start::Revision(r) => Revision::Revision(r),
        }
    }
}

impl From<Revision<u64>> for protocol::subscribe_request::stream::Start {
    fn from(value: Revision<u64>) -> Self {
        match value {
            Revision::Start => protocol::subscribe_request::stream::Start::Beginning(()),
            Revision::End => protocol::subscribe_request::stream::Start::End(()),
            Revision::Revision(r) => protocol::subscribe_request::stream::Start::Revision(r),
        }
    }
}

impl From<protocol::append_stream_response::error::wrong_expected_revision::CurrentRevision>
    for ExpectedRevision
{
    fn from(
        value: protocol::append_stream_response::error::wrong_expected_revision::CurrentRevision,
    ) -> Self {
        match value {
            protocol::append_stream_response::error::wrong_expected_revision::CurrentRevision::NotExists(
                _,
            ) => ExpectedRevision::NoStream,
            protocol::append_stream_response::error::wrong_expected_revision::CurrentRevision::Revision(
                v,
            ) => ExpectedRevision::Revision(v),
        }
    }
}

impl From<protocol::delete_stream_response::error::wrong_expected_revision::CurrentRevision>
    for ExpectedRevision
{
    fn from(
        value: protocol::delete_stream_response::error::wrong_expected_revision::CurrentRevision,
    ) -> Self {
        match value {
            protocol::delete_stream_response::error::wrong_expected_revision::CurrentRevision::NotExists(
                _,
            ) => ExpectedRevision::NoStream,
            protocol::delete_stream_response::error::wrong_expected_revision::CurrentRevision::Revision(
                v,
            ) => ExpectedRevision::Revision(v),
        }
    }
}

impl From<protocol::ContentType> for ContentType {
    fn from(value: protocol::ContentType) -> Self {
        match value {
            protocol::ContentType::Unknown => Self::Unknown,
            protocol::ContentType::Json => Self::Json,
            protocol::ContentType::Binary => Self::Binary,
        }
    }
}

impl From<Propose> for protocol::append_stream_request::Propose {
    fn from(value: Propose) -> Self {
        Self {
            id: Some(value.id.into()),
            content_type: value.content_type as i32,
            class: value.class,
            payload: value.data,
            metadata: Default::default(),
        }
    }
}

impl TryFrom<protocol::append_stream_request::Propose> for Propose {
    type Error = tonic::Status;

    fn try_from(value: protocol::append_stream_request::Propose) -> Result<Self, Self::Error> {
        let id = value
            .id
            .map(Into::into)
            .ok_or_else(|| tonic::Status::invalid_argument("id is missing"))?;

        Ok(Self {
            id,
            content_type: protocol::ContentType::try_from(value.content_type)
                .map(ContentType::from)
                .unwrap_or(ContentType::Unknown),
            class: value.class,
            data: value.payload,
        })
    }
}

impl TryFrom<protocol::RecordedEvent> for Record {
    type Error = tonic::Status;

    fn try_from(value: protocol::RecordedEvent) -> Result<Self, Self::Error> {
        let id = value
            .id
            .map(Into::into)
            .ok_or_else(|| tonic::Status::invalid_argument("id is missing"))?;

        Ok(Self {
            id,
            content_type: protocol::ContentType::try_from(value.content_type)
                .map(ContentType::from)
                .unwrap_or(ContentType::Unknown),
            stream_name: value.stream_name,
            class: value.class,
            position: value.position,
            revision: value.revision,
            data: value.payload,
        })
    }
}

impl From<Record> for protocol::RecordedEvent {
    fn from(value: Record) -> Self {
        Self {
            id: Some(value.id.into()),
            content_type: value.content_type as i32,
            stream_name: value.stream_name,
            class: value.class,
            position: value.position,
            revision: value.revision,
            payload: value.data,
            metadata: Default::default(),
        }
    }
}

impl From<ExpectedRevision> for protocol::append_stream_request::ExpectedRevision {
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(r) => {
                protocol::append_stream_request::ExpectedRevision::Revision(r)
            }
            ExpectedRevision::NoStream => {
                protocol::append_stream_request::ExpectedRevision::NoStream(())
            }
            ExpectedRevision::Any => protocol::append_stream_request::ExpectedRevision::Any(()),
            ExpectedRevision::StreamExists => {
                protocol::append_stream_request::ExpectedRevision::StreamExists(())
            }
        }
    }
}

impl From<ExpectedRevision>
    for protocol::append_stream_response::error::wrong_expected_revision::CurrentRevision
{
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                protocol::append_stream_response::error::wrong_expected_revision::CurrentRevision::Revision(v)
            }
            ExpectedRevision::NoStream => {
                protocol::append_stream_response::error::wrong_expected_revision::CurrentRevision::NotExists(())
            }
            _ => unreachable!(),
        }
    }
}

impl From<ExpectedRevision>
    for protocol::delete_stream_response::error::wrong_expected_revision::CurrentRevision
{
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                protocol::delete_stream_response::error::wrong_expected_revision::CurrentRevision::Revision(v)
            }
            ExpectedRevision::NoStream => {
                protocol::delete_stream_response::error::wrong_expected_revision::CurrentRevision::NotExists(())
            }
            _ => unreachable!(),
        }
    }
}

impl From<ExpectedRevision>
    for protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision
{
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::Expected(v)
            }
            ExpectedRevision::NoStream => {
                protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::NoStream(())
            }
            ExpectedRevision::Any => {
                protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::Any(())
            }
            ExpectedRevision::StreamExists => {
                protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::StreamExists(())
            }
        }
    }
}

impl From<ExpectedRevision>
    for protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision
{
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(v) => {
                protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::Expected(v)
            }
            ExpectedRevision::NoStream => {
                protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::NoStream(())
            }
            ExpectedRevision::Any => {
                protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::Any(())
            }
            ExpectedRevision::StreamExists => {
                protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::StreamExists(())
            }
        }
    }
}

impl From<protocol::append_stream_request::ExpectedRevision> for ExpectedRevision {
    fn from(value: protocol::append_stream_request::ExpectedRevision) -> Self {
        match value {
            protocol::append_stream_request::ExpectedRevision::Revision(r) => {
                ExpectedRevision::Revision(r)
            }
            protocol::append_stream_request::ExpectedRevision::NoStream(_) => {
                ExpectedRevision::NoStream
            }
            protocol::append_stream_request::ExpectedRevision::Any(_) => ExpectedRevision::Any,
            protocol::append_stream_request::ExpectedRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
            }
        }
    }
}

impl From<protocol::delete_stream_request::ExpectedRevision> for ExpectedRevision {
    fn from(value: protocol::delete_stream_request::ExpectedRevision) -> Self {
        match value {
            protocol::delete_stream_request::ExpectedRevision::Revision(r) => {
                ExpectedRevision::Revision(r)
            }
            protocol::delete_stream_request::ExpectedRevision::NoStream(_) => {
                ExpectedRevision::NoStream
            }
            protocol::delete_stream_request::ExpectedRevision::Any(_) => ExpectedRevision::Any,
            protocol::delete_stream_request::ExpectedRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
            }
        }
    }
}

impl From<ExpectedRevision> for protocol::delete_stream_request::ExpectedRevision {
    fn from(value: ExpectedRevision) -> Self {
        match value {
            ExpectedRevision::Revision(r) => {
                protocol::delete_stream_request::ExpectedRevision::Revision(r)
            }
            ExpectedRevision::NoStream => {
                protocol::delete_stream_request::ExpectedRevision::NoStream(())
            }
            ExpectedRevision::Any => protocol::delete_stream_request::ExpectedRevision::Any(()),
            ExpectedRevision::StreamExists => {
                protocol::delete_stream_request::ExpectedRevision::StreamExists(())
            }
        }
    }
}

impl From<protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision>
    for ExpectedRevision
{
    fn from(
        value: protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision,
    ) -> Self {
        match value {
            protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::Any(_) => {
                ExpectedRevision::Any
            }
            protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
            }
            protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::NoStream(
                _,
            ) => ExpectedRevision::NoStream,
            protocol::append_stream_response::error::wrong_expected_revision::ExpectedRevision::Expected(
                v,
            ) => ExpectedRevision::Revision(v),
        }
    }
}

impl From<protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision>
    for ExpectedRevision
{
    fn from(
        value: protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision,
    ) -> Self {
        match value {
            protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::Any(_) => {
                ExpectedRevision::Any
            }
            protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::StreamExists(_) => {
                ExpectedRevision::StreamExists
            }
            protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::NoStream(
                _,
            ) => ExpectedRevision::NoStream,
            protocol::delete_stream_response::error::wrong_expected_revision::ExpectedRevision::Expected(
                v,
            ) => ExpectedRevision::Revision(v),
        }
    }
}

impl From<WrongExpectedRevisionError>
    for protocol::append_stream_response::error::WrongExpectedRevision
{
    fn from(value: WrongExpectedRevisionError) -> Self {
        Self {
            current_revision: Some(value.current.into()),
            expected_revision: Some(value.expected.into()),
        }
    }
}

impl From<WrongExpectedRevisionError>
    for protocol::delete_stream_response::error::WrongExpectedRevision
{
    fn from(value: WrongExpectedRevisionError) -> Self {
        Self {
            current_revision: Some(value.current.into()),
            expected_revision: Some(value.expected.into()),
        }
    }
}

impl From<SubscribeToStream> for protocol::subscribe_request::Stream {
    fn from(value: SubscribeToStream) -> Self {
        Self {
            stream_name: value.stream_name,
            start: Some(value.start.into()),
        }
    }
}

impl TryFrom<protocol::subscribe_request::Stream> for SubscribeToStream {
    type Error = tonic::Status;

    fn try_from(value: protocol::subscribe_request::Stream) -> Result<Self, Self::Error> {
        let start = value
            .start
            .map(Into::into)
            .ok_or_else(|| tonic::Status::invalid_argument("start is missing"))?;

        Ok(Self {
            stream_name: value.stream_name,
            start,
        })
    }
}

impl From<SubscribeToProgram> for protocol::subscribe_request::Program {
    fn from(value: SubscribeToProgram) -> Self {
        Self {
            name: value.name,
            source: value.source,
        }
    }
}

impl From<protocol::subscribe_request::Program> for SubscribeToProgram {
    fn from(value: protocol::subscribe_request::Program) -> Self {
        Self {
            name: value.name,
            source: value.source,
        }
    }
}

impl TryFrom<protocol::AppendStreamResponse> for AppendStreamCompleted {
    type Error = tonic::Status;

    fn try_from(value: protocol::AppendStreamResponse) -> Result<Self, tonic::Status> {
        let append_result = value
            .append_result
            .ok_or_else(|| tonic::Status::invalid_argument("append_result is missing"))?;

        match append_result {
            protocol::append_stream_response::AppendResult::WriteResult(r) => {
                Ok(AppendStreamCompleted::Success(WriteResult {
                    next_expected_version: ExpectedRevision::Revision(r.next_revision),
                    position: r.position,
                    next_logical_position: 0,
                }))
            }

            protocol::append_stream_response::AppendResult::Error(e) => {
                let error = e
                    .error
                    .ok_or_else(|| tonic::Status::invalid_argument("error is missing"))?;

                match error {
                    protocol::append_stream_response::error::Error::WrongRevision(e) => {
                        let expected = e.expected_revision.map(Into::into).ok_or_else(|| {
                            tonic::Status::invalid_argument("expected_revision is missing")
                        })?;
                        let current = e.current_revision.map(Into::into).ok_or_else(|| {
                            tonic::Status::invalid_argument("current_revision is missing")
                        })?;

                        Ok(AppendStreamCompleted::Error(
                            AppendError::WrongExpectedRevision(WrongExpectedRevisionError {
                                expected,
                                current,
                            }),
                        ))
                    }
                    protocol::append_stream_response::error::Error::StreamDeleted(_) => {
                        Ok(AppendStreamCompleted::Error(AppendError::StreamDeleted))
                    }
                }
            }
        }
    }
}

impl From<AppendStreamCompleted> for protocol::AppendStreamResponse {
    fn from(value: AppendStreamCompleted) -> Self {
        match value {
            AppendStreamCompleted::Success(w) => protocol::AppendStreamResponse {
                append_result: Some(protocol::append_stream_response::AppendResult::WriteResult(
                    w.into(),
                )),
            },

            AppendStreamCompleted::Error(e) => protocol::AppendStreamResponse {
                append_result: Some(protocol::append_stream_response::AppendResult::Error(
                    protocol::append_stream_response::Error {
                        error: Some(match e {
                            AppendError::WrongExpectedRevision(e) => {
                                protocol::append_stream_response::error::Error::WrongRevision(
                                    e.into(),
                                )
                            }
                            AppendError::StreamDeleted => {
                                protocol::append_stream_response::error::Error::StreamDeleted(())
                            }
                        }),
                    },
                )),
            },
        }
    }
}

impl From<WriteResult> for protocol::append_stream_response::WriteResult {
    fn from(value: WriteResult) -> Self {
        Self {
            next_revision: value.next_expected_version.raw() as u64,
            position: value.position,
        }
    }
}

impl From<WriteResult> for protocol::delete_stream_response::DeleteResult {
    fn from(value: WriteResult) -> Self {
        Self {
            next_revision: value.next_expected_version.raw() as u64,
            position: value.position,
        }
    }
}

impl TryFrom<protocol::ReadStreamResponse> for ReadStreamResponse {
    type Error = tonic::Status;

    fn try_from(value: protocol::ReadStreamResponse) -> Result<Self, Self::Error> {
        let read_result = value
            .read_result
            .ok_or_else(|| tonic::Status::invalid_argument("read_result is missing"))?;

        match read_result {
            protocol::read_stream_response::ReadResult::EndOfStream(_) => {
                Ok(ReadStreamResponse::EndOfStream)
            }
            protocol::read_stream_response::ReadResult::EventAppeared(e) => {
                Ok(ReadStreamResponse::EventAppeared(e.try_into()?))
            }
        }
    }
}

impl TryFrom<ReadStreamResponse> for protocol::ReadStreamResponse {
    type Error = ReadError;

    fn try_from(value: ReadStreamResponse) -> Result<Self, Self::Error> {
        match value {
            ReadStreamResponse::EndOfStream => Ok(protocol::ReadStreamResponse {
                read_result: Some(protocol::read_stream_response::ReadResult::EndOfStream(())),
            }),

            ReadStreamResponse::EventAppeared(e) => Ok(protocol::ReadStreamResponse {
                read_result: Some(protocol::read_stream_response::ReadResult::EventAppeared(
                    e.into(),
                )),
            }),

            ReadStreamResponse::StreamDeleted => Err(ReadError::StreamDeleted),
        }
    }
}

impl TryFrom<protocol::DeleteStreamResponse> for DeleteStreamCompleted {
    type Error = tonic::Status;

    fn try_from(value: protocol::DeleteStreamResponse) -> Result<Self, tonic::Status> {
        let result = value
            .result
            .ok_or_else(|| tonic::Status::invalid_argument("result is missing"))?;

        match result {
            protocol::delete_stream_response::Result::WriteResult(r) => {
                Ok(DeleteStreamCompleted::Success(WriteResult {
                    next_expected_version: ExpectedRevision::Revision(r.next_revision),
                    position: r.position,
                    next_logical_position: 0,
                }))
            }

            protocol::delete_stream_response::Result::Error(e) => {
                let error = e
                    .error
                    .ok_or_else(|| tonic::Status::invalid_argument("error is missing"))?;
                match error {
                    protocol::delete_stream_response::error::Error::WrongRevision(e) => {
                        let expected = e.expected_revision.map(Into::into).ok_or_else(|| {
                            tonic::Status::invalid_argument("expected_revision is missing")
                        })?;
                        let current = e.current_revision.map(Into::into).ok_or_else(|| {
                            tonic::Status::invalid_argument("current_revision is missing")
                        })?;

                        Ok(DeleteStreamCompleted::Error(
                            DeleteError::WrongExpectedRevision(WrongExpectedRevisionError {
                                expected,
                                current,
                            }),
                        ))
                    }

                    protocol::delete_stream_response::error::Error::NotLeader(e) => Ok(
                        DeleteStreamCompleted::Error(DeleteError::NotLeaderException(EndPoint {
                            host: e.leader_host,
                            port: e.leader_port as u16,
                        })),
                    ),

                    protocol::delete_stream_response::error::Error::StreamDeleted(_) => {
                        Ok(DeleteStreamCompleted::Error(DeleteError::StreamDeleted))
                    }
                }
            }
        }
    }
}

impl From<DeleteStreamCompleted> for protocol::DeleteStreamResponse {
    fn from(value: DeleteStreamCompleted) -> Self {
        match value {
            DeleteStreamCompleted::Success(w) => protocol::DeleteStreamResponse {
                result: Some(protocol::delete_stream_response::Result::WriteResult(
                    w.into(),
                )),
            },

            DeleteStreamCompleted::Error(e) => protocol::DeleteStreamResponse {
                result: Some(protocol::delete_stream_response::Result::Error(
                    protocol::delete_stream_response::Error {
                        error: Some(match e {
                            DeleteError::WrongExpectedRevision(e) => {
                                protocol::delete_stream_response::error::Error::WrongRevision(
                                    e.into(),
                                )
                            }

                            DeleteError::NotLeaderException(e) => {
                                protocol::delete_stream_response::error::Error::NotLeader(
                                    protocol::delete_stream_response::error::NotLeader {
                                        leader_host: e.host,
                                        leader_port: e.port as u32,
                                    },
                                )
                            }

                            DeleteError::StreamDeleted => {
                                protocol::delete_stream_response::error::Error::StreamDeleted(())
                            }
                        }),
                    },
                )),
            },
        }
    }
}

impl TryFrom<protocol::subscribe_response::Notification> for SubscriptionNotification {
    type Error = tonic::Status;

    fn try_from(value: protocol::subscribe_response::Notification) -> Result<Self, Self::Error> {
        let kind = value
            .kind
            .ok_or_else(|| tonic::Status::invalid_argument("kind is missing"))?;
        match kind {
            protocol::subscribe_response::notification::Kind::Subscribed(s) => {
                Ok(Self::Subscribed(s))
            }
            protocol::subscribe_response::notification::Kind::Unsubscribed(s) => {
                Ok(Self::Unsubscribed(s))
            }
        }
    }
}

impl From<SubscriptionNotification> for protocol::subscribe_response::Notification {
    fn from(value: SubscriptionNotification) -> Self {
        protocol::subscribe_response::Notification {
            kind: Some(match value {
                SubscriptionNotification::Subscribed(s) => {
                    protocol::subscribe_response::notification::Kind::Subscribed(s)
                }

                SubscriptionNotification::Unsubscribed(s) => {
                    protocol::subscribe_response::notification::Kind::Unsubscribed(s)
                }
            }),
        }
    }
}

impl TryFrom<protocol::SubscribeResponse> for SubscriptionEvent {
    type Error = tonic::Status;

    fn try_from(value: protocol::SubscribeResponse) -> Result<Self, Self::Error> {
        let event = value
            .event
            .ok_or_else(|| tonic::Status::invalid_argument("event is missing"))?;

        match event {
            protocol::subscribe_response::Event::Confirmation(c) => {
                let kind = c
                    .kind
                    .ok_or_else(|| tonic::Status::invalid_argument("kind is missing"))?;

                match kind {
                    protocol::subscribe_response::confirmation::Kind::StreamName(s) => Ok(
                        SubscriptionEvent::Confirmed(SubscriptionConfirmation::StreamName(s)),
                    ),
                    protocol::subscribe_response::confirmation::Kind::ProcessId(p) => Ok(
                        SubscriptionEvent::Confirmed(SubscriptionConfirmation::ProcessId(p)),
                    ),
                }
            }
            protocol::subscribe_response::Event::EventAppeared(e) => {
                let event = e
                    .event
                    .ok_or_else(|| tonic::Status::invalid_argument("event is missing"))?;
                Ok(SubscriptionEvent::EventAppeared(event.try_into()?))
            }
            protocol::subscribe_response::Event::CaughtUp(_) => Ok(SubscriptionEvent::CaughtUp),
            protocol::subscribe_response::Event::Error(_) => {
                Ok(SubscriptionEvent::Unsubscribed(UnsubscribeReason::Server))
            }
            protocol::subscribe_response::Event::Notification(n) => {
                Ok(SubscriptionEvent::Notification(n.try_into()?))
            }
        }
    }
}

impl From<SubscriptionEvent> for protocol::SubscribeResponse {
    fn from(value: SubscriptionEvent) -> Self {
        match value {
            SubscriptionEvent::Confirmed(c) => match c {
                SubscriptionConfirmation::StreamName(s) => protocol::SubscribeResponse {
                    event: Some(protocol::subscribe_response::Event::Confirmation(
                        protocol::subscribe_response::Confirmation {
                            kind: Some(
                                protocol::subscribe_response::confirmation::Kind::StreamName(s),
                            ),
                        },
                    )),
                },
                SubscriptionConfirmation::ProcessId(p) => protocol::SubscribeResponse {
                    event: Some(protocol::subscribe_response::Event::Confirmation(
                        protocol::subscribe_response::Confirmation {
                            kind: Some(
                                protocol::subscribe_response::confirmation::Kind::ProcessId(p),
                            ),
                        },
                    )),
                },
            },
            SubscriptionEvent::EventAppeared(e) => protocol::SubscribeResponse {
                event: Some(protocol::subscribe_response::Event::EventAppeared(
                    protocol::subscribe_response::EventAppeared {
                        event: Some(e.into()),
                    },
                )),
            },
            SubscriptionEvent::CaughtUp => protocol::SubscribeResponse {
                event: Some(protocol::subscribe_response::Event::CaughtUp(
                    protocol::subscribe_response::CaughtUp {},
                )),
            },
            SubscriptionEvent::Unsubscribed(_) => protocol::SubscribeResponse {
                event: Some(protocol::subscribe_response::Event::Error(
                    protocol::subscribe_response::Error {},
                )),
            },

            SubscriptionEvent::Notification(n) => protocol::SubscribeResponse {
                event: Some(protocol::subscribe_response::Event::Notification(n.into())),
            },
        }
    }
}

impl TryFrom<protocol::list_programs_response::ProgramSummary> for ProgramSummary {
    type Error = tonic::Status;

    fn try_from(
        value: protocol::list_programs_response::ProgramSummary,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            name: value.name,
            started_at: Utc
                .timestamp_opt(value.started_at, 0)
                .single()
                .ok_or_else(|| tonic::Status::invalid_argument("started_at is out of range"))?,
        })
    }
}

impl From<ProgramSummary> for protocol::list_programs_response::ProgramSummary {
    fn from(value: ProgramSummary) -> Self {
        Self {
            id: value.id,
            name: value.name,
            started_at: value.started_at.timestamp(),
        }
    }
}

impl TryFrom<protocol::ListProgramsResponse> for ProgramListed {
    type Error = tonic::Status;

    fn try_from(value: protocol::ListProgramsResponse) -> Result<Self, Self::Error> {
        let programs: Result<Vec<ProgramSummary>, tonic::Status> =
            value.programs.into_iter().map(|p| p.try_into()).collect();
        Ok(Self {
            programs: programs?,
        })
    }
}

impl From<ProgramListed> for protocol::ListProgramsResponse {
    fn from(value: ProgramListed) -> Self {
        Self {
            programs: value.programs.into_iter().map(|p| p.into()).collect(),
        }
    }
}

impl From<GetProgramStats> for protocol::ProgramStatsRequest {
    fn from(value: GetProgramStats) -> Self {
        Self { id: value.id }
    }
}

impl From<protocol::ProgramStatsRequest> for GetProgramStats {
    fn from(value: protocol::ProgramStatsRequest) -> Self {
        Self { id: value.id }
    }
}

impl From<KillProgram> for protocol::StopProgramRequest {
    fn from(value: KillProgram) -> Self {
        Self { id: value.id }
    }
}

impl From<protocol::StopProgramRequest> for KillProgram {
    fn from(value: protocol::StopProgramRequest) -> Self {
        Self { id: value.id }
    }
}

impl TryFrom<protocol::StopProgramResponse> for ProgramKilled {
    type Error = tonic::Status;

    fn try_from(value: protocol::StopProgramResponse) -> Result<Self, tonic::Status> {
        let result = value
            .result
            .ok_or_else(|| tonic::Status::invalid_argument("result is missing"))?;

        match result {
            protocol::stop_program_response::Result::Success(_) => Ok(ProgramKilled::Success),
            protocol::stop_program_response::Result::Error(e) => {
                let error = e
                    .error
                    .ok_or_else(|| tonic::Status::invalid_argument("error is missing"))?;

                match error {
                    protocol::stop_program_response::error::Error::NotExists(_) => {
                        Ok(ProgramKilled::Error(ProgramKillError::NotExists))
                    }
                }
            }
        }
    }
}

impl From<ProgramKilled> for protocol::StopProgramResponse {
    fn from(value: ProgramKilled) -> Self {
        match value {
            ProgramKilled::Success => protocol::StopProgramResponse {
                result: Some(protocol::stop_program_response::Result::Success(())),
            },

            ProgramKilled::Error(e) => protocol::StopProgramResponse {
                result: Some(protocol::stop_program_response::Result::Error(
                    protocol::stop_program_response::Error {
                        error: Some(match e {
                            ProgramKillError::NotExists => {
                                protocol::stop_program_response::error::Error::NotExists(())
                            }
                        }),
                    },
                )),
            },
        }
    }
}

impl TryFrom<protocol::ProgramStatsResponse> for ProgramObtained {
    type Error = tonic::Status;

    fn try_from(value: protocol::ProgramStatsResponse) -> Result<Self, tonic::Status> {
        let result = value
            .result
            .ok_or_else(|| tonic::Status::invalid_argument("result is missing"))?;

        match result {
            protocol::program_stats_response::Result::Program(stats) => {
                Ok(ProgramObtained::Success(stats.try_into()?))
            }

            protocol::program_stats_response::Result::Error(e) => {
                let error = e
                    .error
                    .ok_or_else(|| tonic::Status::invalid_argument("error is missing"))?;

                match error {
                    protocol::program_stats_response::error::Error::NotExists(_) => {
                        Ok(ProgramObtained::Error(GetProgramError::NotExists))
                    }
                }
            }
        }
    }
}

impl From<ProgramObtained> for protocol::ProgramStatsResponse {
    fn from(value: ProgramObtained) -> Self {
        match value {
            ProgramObtained::Success(stats) => protocol::ProgramStatsResponse {
                result: Some(protocol::program_stats_response::Result::Program(
                    stats.into(),
                )),
            },

            ProgramObtained::Error(e) => protocol::ProgramStatsResponse {
                result: Some(protocol::program_stats_response::Result::Error(
                    protocol::program_stats_response::Error {
                        error: Some(match e {
                            GetProgramError::NotExists => {
                                protocol::program_stats_response::error::Error::NotExists(())
                            }
                        }),
                    },
                )),
            },
        }
    }
}

impl TryFrom<protocol::program_stats_response::ProgramStats> for ProgramStats {
    type Error = tonic::Status;

    fn try_from(
        value: protocol::program_stats_response::ProgramStats,
    ) -> Result<Self, tonic::Status> {
        Ok(Self {
            id: value.id,
            name: value.name,
            source_code: value.source_code,
            subscriptions: value.subscriptions,
            pushed_events: value.pushed_events as usize,
            started: Utc
                .timestamp_opt(value.started_at, 0)
                .single()
                .ok_or_else(|| tonic::Status::invalid_argument("started_at is out of range"))?,
        })
    }
}

impl From<ProgramStats> for protocol::program_stats_response::ProgramStats {
    fn from(value: ProgramStats) -> Self {
        Self {
            id: value.id,
            name: value.name,
            source_code: value.source_code,
            subscriptions: value.subscriptions,
            pushed_events: value.pushed_events as u64,
            started_at: value.started.timestamp(),
        }
    }
}

impl From<ListPrograms> for protocol::ListProgramsRequest {
    fn from(_: ListPrograms) -> Self {
        Self { empty: None }
    }
}

impl From<protocol::ListProgramsRequest> for ListPrograms {
    fn from(_: protocol::ListProgramsRequest) -> Self {
        Self {}
    }
}
