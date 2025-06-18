use crate::process::{Item, ManagerClient, Proc, ProcId, ProcessEnv, RequestContext};
use tokio::sync::mpsc::UnboundedReceiver;

use super::messages::{Messages, TestSinkRequests, TestSinkResponses};

pub async fn run(mut env: ProcessEnv) -> eyre::Result<()> {
    while let Some(item) = env.queue.recv().await {
        if let Item::Stream(stream) = item {
            if let Ok(TestSinkRequests::StreamFrom { low, high }) = stream.payload.try_into() {
                for num in low..high {
                    if stream
                        .sender
                        .send(TestSinkResponses::Stream(num).into())
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

pub struct SinkClient {
    target: ProcId,
    inner: ManagerClient,
}

impl SinkClient {
    pub async fn resolve(inner: ManagerClient) -> eyre::Result<Self> {
        Ok(Self {
            target: inner.wait_for(Proc::Sink).await?.must_succeed()?,
            inner,
        })
    }

    pub async fn stream_from(&self, low: u64, high: u64) -> eyre::Result<Streaming> {
        let inner = self
            .inner
            .request_stream(
                RequestContext::new(),
                self.target,
                TestSinkRequests::StreamFrom { low, high }.into(),
            )
            .await?;

        Ok(Streaming { inner })
    }
}

pub struct Streaming {
    inner: UnboundedReceiver<Messages>,
}

impl Streaming {
    pub async fn next(&mut self) -> eyre::Result<Option<u64>> {
        if let Some(resp) = self.inner.recv().await {
            if let Ok(TestSinkResponses::Stream(value)) = resp.try_into() {
                return Ok(Some(value));
            }

            eyre::bail!("unexpected message when streaming from the sink process");
        }

        Ok(None)
    }
}
