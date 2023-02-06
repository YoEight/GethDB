use eyre::bail;
use geth_mikoshi::Mikoshi;
use tokio::sync::{
    mpsc::{self, UnboundedReceiver},
    oneshot,
};

use crate::messages::{AppendStream, AppendStreamCompleted, ReadStream, ReadStreamCompleted};
enum Msg {
    ReadStream(ReadStream, oneshot::Sender<ReadStreamCompleted>),
    AppendStream(AppendStream, oneshot::Sender<AppendStreamCompleted>),
}

pub struct StorageClient {
    inner: mpsc::UnboundedSender<Msg>,
}

impl StorageClient {
    pub fn read_stream(
        &self,
        msg: ReadStream,
        callback: oneshot::Sender<ReadStreamCompleted>,
    ) -> eyre::Result<()> {
        if self.inner.send(Msg::ReadStream(msg, callback)).is_err() {
            bail!("Main bus has shutdown!");
        }

        Ok(())
    }

    pub fn append_stream(
        &self,
        msg: AppendStream,
        callback: oneshot::Sender<AppendStreamCompleted>,
    ) -> eyre::Result<()> {
        if self.inner.send(Msg::AppendStream(msg, callback)).is_err() {
            bail!("Main bus has shutdown!");
        }

        Ok(())
    }
}

pub fn start() -> StorageClient {
    let (sender, mailbox) = mpsc::unbounded_channel();

    tokio::spawn(service(mailbox));

    StorageClient { inner: sender }
}

async fn service(mut mailbox: UnboundedReceiver<Msg>) {
    let mut mikoshi = Mikoshi::in_memory();

    while let Some(msg) = mailbox.recv().await {
        match msg {
            Msg::ReadStream(params, callback) => {
                let reader = mikoshi.read(params.stream_name, params.starting, params.direction);
                let _ = callback.send(ReadStreamCompleted {
                    correlation: params.correlation,
                    reader,
                });
            }

            Msg::AppendStream(params, callback) => {
                let result = mikoshi.append(params.stream_name, params.expected, params.events);
                let _ = callback.send(AppendStreamCompleted {
                    correlation: params.correlation,
                    result,
                });
            }
        }
    }
}
