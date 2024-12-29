use crate::process::{Item, ManagerClient, Proc, ProcId, ProcessEnv};
use bytes::{Buf, BufMut, BytesMut};
use tokio::sync::mpsc::UnboundedReceiver;

pub async fn run(mut env: ProcessEnv) -> eyre::Result<()> {
    while let Some(item) = env.queue.recv().await {
        if let Item::Stream(mut stream) = item {
            let mut buffer = env.client.pool.get().await.unwrap();
            let low = stream.payload.get_u64_le();
            let high = stream.payload.get_u64_le();

            for num in low..high {
                buffer.put_u64_le(num);
                if stream.sender.send(buffer.split().freeze()).is_err() {
                    break;
                }
            }
        }
    }

    Ok(())
}

pub struct SinkClient {
    target: ProcId,
    inner: ManagerClient,
    buffer: BytesMut,
}

impl SinkClient {
    pub async fn resolve(inner: ManagerClient) -> eyre::Result<Self> {
        Ok(Self {
            target: inner.wait_for(Proc::Sink).await?,
            inner,
            buffer: BytesMut::new(),
        })
    }

    pub async fn stream_from(&mut self, low: u64, high: u64) -> eyre::Result<Streaming> {
        self.buffer.put_u64_le(low);
        self.buffer.put_u64_le(high);

        let inner = self
            .inner
            .request_stream(self.target, self.buffer.split().freeze())
            .await?;

        Ok(Streaming { inner })
    }
}

pub struct Streaming {
    inner: UnboundedReceiver<bytes::Bytes>,
}

impl Streaming {
    pub async fn next(&mut self) -> Option<u64> {
        if let Some(mut bytes) = self.inner.recv().await {
            Some(bytes.get_u64_le())
        } else {
            None
        }
    }
}
