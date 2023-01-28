use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use super::generated::geth::protocol::server::{
    streams_server::Streams, AppendReq, AppendResp, BatchAppendReq, BatchAppendResp, DeleteReq,
    DeleteResp, ReadReq, ReadResp, TombstoneReq, TombstoneResp,
};

use futures::stream::BoxStream;

#[derive(Default)]
pub struct StreamsImpl {}

#[tonic::async_trait]
impl Streams for StreamsImpl {
    type ReadStream = BoxStream<'static, Result<ReadResp, Status>>;
    type BatchAppendStream = BoxStream<'static, Result<BatchAppendResp, Status>>;

    async fn read(&self, request: Request<ReadReq>) -> Result<Response<Self::ReadStream>, Status> {
        let req = request.into_inner();

        let options = req
            .options
            .ok_or(Status::invalid_argument("Options was not provided"))?;

        // let options = if let Some(options) = req.options {
        //     options
        // } else {
        // }

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

    async fn batch_append(
        &self,
        request: Request<Streaming<BatchAppendReq>>,
    ) -> Result<Response<Self::BatchAppendStream>, Status> {
        todo!()
    }
}
