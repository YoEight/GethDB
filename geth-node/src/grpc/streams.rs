use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use super::generated::geth::protocol::{
    server::{
        streams_server::Streams, AppendReq, AppendResp, BatchAppendReq, BatchAppendResp, DeleteReq,
        DeleteResp, ReadReq, ReadResp, TombstoneReq, TombstoneResp,
    },
    StreamIdentifier,
};

use crate::grpc::generated::geth::protocol::server::read_req::options::stream_options::RevisionOption;
use crate::grpc::generated::geth::protocol::server::read_req::options::{
    CountOption, StreamOption,
};
use crate::messages::ReadStream;
use crate::types::{Direction, ExpectedRevision, Revision};
use futures::stream::BoxStream;
use uuid::Uuid;

#[derive(Default)]
pub struct StreamsImpl {}

#[tonic::async_trait]
impl Streams for StreamsImpl {
    type ReadStream = BoxStream<'static, Result<ReadResp, Status>>;
    async fn read(&self, request: Request<ReadReq>) -> Result<Response<Self::ReadStream>, Status> {
        let req = request.into_inner();

        let options = req
            .options
            .ok_or(Status::invalid_argument("Options was not provided"))?;

        let stream_name = options
            .stream_option
            .ok_or(Status::invalid_argument("Stream name argument is missing"))?;

        let (stream_name, revision) = match stream_name {
            StreamOption::Stream(options) => {
                let ident = if let Some(ident) = options.stream_identifier {
                    String::from_utf8(ident.stream_name)
                        .map_err(|_| Status::invalid_argument("Stream name is not valid UTF-8"))?
                } else {
                    return Err(Status::invalid_argument("Stream name argument is missing"));
                };

                let revision = options
                    .revision_option
                    .ok_or(Status::invalid_argument("revision option is not provided"))?;

                let revision = match revision {
                    RevisionOption::Revision(rev) => Revision::Revision(rev),
                    RevisionOption::Start(_) => Revision::Start,
                    RevisionOption::End(_) => Revision::End,
                };

                (ident, revision)
            }

            StreamOption::All(_) => {
                return Err(Status::unimplemented(
                    "Reading from $all is not implemented yet",
                ));
            }
        };

        let direction = Direction::try_from(options.read_direction)
            .map_err(|_| Status::invalid_argument("Wrong direction value"))?;

        let count = options
            .count_option
            .ok_or(Status::invalid_argument("count_options was not provided"))?;

        let count = match count {
            CountOption::Count(c) => c as usize,
            CountOption::Subscription(_) => {
                return Err(Status::unimplemented(
                    "Subscriptions are not implemented yet",
                ));
            }
        };

        let msg = ReadStream {
            correlation: Uuid::new_v4(),
            starting: revision,
            count,
            stream_name,
            direction,
        };

        Err(Status::invalid_argument("Options was not provided"))
    }

    async fn append(
        &self,
        request: Request<Streaming<AppendReq>>,
    ) -> Result<Response<AppendResp>, Status> {
        todo!()
    }

    async fn delete(&self, request: Request<DeleteReq>) -> Result<Response<DeleteResp>, Status> {
        todo!()
    }

    async fn tombstone(
        &self,
        request: Request<TombstoneReq>,
    ) -> Result<Response<TombstoneResp>, Status> {
        todo!()
    }

    type BatchAppendStream = BoxStream<'static, Result<BatchAppendResp, Status>>;

    async fn batch_append(
        &self,
        request: Request<Streaming<BatchAppendReq>>,
    ) -> Result<Response<Self::BatchAppendStream>, Status> {
        todo!()
    }
}
