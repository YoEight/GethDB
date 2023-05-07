use eyre::bail;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};

use crate::messages::{AppendStream, AppendStreamCompleted, ReadStream, ReadStreamCompleted};

pub enum Msg {
    ReadStream(ReadStreamMsg),
    AppendStream(AppendStreamMsg),
}

pub struct ReadStreamMsg {
    pub payload: ReadStream,
    pub mail: oneshot::Sender<ReadStreamCompleted>,
}

pub struct AppendStreamMsg {
    pub payload: AppendStream,
    pub mail: oneshot::Sender<AppendStreamCompleted>,
}

#[derive(Clone)]
pub struct Bus {
    inner: Sender<Msg>,
}

impl Bus {
    pub async fn read_stream(&self, msg: ReadStream) -> eyre::Result<ReadStreamCompleted> {
        let (sender, recv) = oneshot::channel();
        if self
            .inner
            .send(Msg::ReadStream(ReadStreamMsg {
                payload: msg,
                mail: sender,
            }))
            .await
            .is_err()
        {
            bail!("Main bus has shutdown!");
        }

        if let Ok(resp) = recv.await {
            return Ok(resp);
        }

        bail!("Main bus has shutdown!");
    }

    pub async fn append_stream(&self, msg: AppendStream) -> eyre::Result<AppendStreamCompleted> {
        let (sender, recv) = oneshot::channel();
        if self
            .inner
            .send(Msg::AppendStream(AppendStreamMsg {
                payload: msg,
                mail: sender,
            }))
            .await
            .is_err()
        {
            bail!("Main bus has shutdown!");
        }

        if let Ok(resp) = recv.await {
            return Ok(resp);
        }

        bail!("Main bus has shutdown!");
    }
}

pub struct Mailbox {
    inner: Receiver<Msg>,
}

impl Mailbox {
    pub async fn next(&mut self) -> Option<Msg> {
        self.inner.recv().await
    }
}

pub fn new_bus(buffer: usize) -> (Bus, Mailbox) {
    let (sender, recv) = mpsc::channel(buffer);

    (Bus { inner: sender }, Mailbox { inner: recv })
}
