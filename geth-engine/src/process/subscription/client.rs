use crate::messages::ReadStreamCompleted;
use crate::process::reading::{Request, Response, Streaming};
use crate::process::{ManagerClient, ProcId, ProcessEnv};
use bytes::BytesMut;
use geth_common::{Direction, Revision};

pub struct SubscriptionClient {
    target: ProcId,
    inner: ManagerClient,
    buffer: BytesMut,
}

impl SubscriptionClient {
    pub fn new(target: ProcId, inner: ManagerClient, buffer: BytesMut) -> Self {
        Self {
            target,
            inner,
            buffer,
        }
    }

    pub async fn resolve(env: &mut ProcessEnv) -> eyre::Result<Self> {
        let proc_id = env.client.wait_for("subscription").await?;
        Ok(Self::new(proc_id, env.client.clone(), env.buffer.split()))
    }

    pub async fn read(
        &mut self,
        stream_name: &str,
        start: Revision<u64>,
    ) -> eyre::Result<ReadStreamCompleted> {
        //     let mut mailbox = self
        //         .inner
        //         .request_stream(
        //             self.target,
        //             Request::read(&mut self.buffer, stream_name, start, direction, count),
        //         )
        //         .await?;
        //
        //     if let Some(resp) = mailbox.recv().await {
        //         if let Some(resp) = Response::try_from(resp) {
        //             match resp {
        //                 Response::Error => {
        //                     eyre::bail!("internal error");
        //                 }
        //
        //                 Response::StreamDeleted => {
        //                     return Ok(ReadStreamCompleted::StreamDeleted);
        //                 }
        //
        //                 Response::Streaming => {
        //                     return Ok(ReadStreamCompleted::Success(Streaming {
        //                         inner: mailbox,
        //                         batch: None,
        //                     }));
        //                 }
        //             }
        //         }
        //
        //         eyre::bail!("protocol error when communicating with the reader process");
        //     }
        //
        //     eyre::bail!("reader process is no longer running")
        todo!()
    }
}
