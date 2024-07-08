use std::collections::BTreeMap;

use eyre::Context;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

use geth_common::Record;

use crate::bus::SubscribeMsg;
use crate::messages::{StreamTarget, SubscribeTo, SubscriptionRequestOutcome, SubscriptionTarget};
use crate::names;
use crate::process::SubscriptionsClient;

pub struct Request {
    pub position: u64,
    pub callback: oneshot::Sender<()>,
}

pub enum Cmd {
    Apply(Record),
    Register(Request),
}

#[derive(Clone)]
pub struct WriteRequestManagerClient {
    inner: UnboundedSender<Cmd>,
}

impl WriteRequestManagerClient {
    pub async fn wait_until_indexing_reach(&self, position: u64) -> eyre::Result<()> {
        let (callback, recv) = oneshot::channel();

        self.inner
            .send(Cmd::Register(Request { position, callback }))
            .wrap_err("write-request manager is unreachable")?;

        recv.await
            .wrap_err("unexpected error when waiting append request to be indexed")?;

        Ok(())
    }
}

pub fn start(sub_client: SubscriptionsClient) -> WriteRequestManagerClient {
    let (inner, mailbox) = unbounded_channel();

    tokio::spawn(indexing_subscription(sub_client, inner.clone()));
    tokio::spawn(manager_process(mailbox));

    WriteRequestManagerClient { inner }
}

#[derive(Default)]
struct WriteRequestManager {
    position: u64,
    inner: BTreeMap<u64, Request>,
}

impl WriteRequestManager {
    fn collect_keys(&self, up_to: u64) -> Vec<u64> {
        self.inner.range(..=up_to).map(|(k, _)| *k).collect()
    }

    pub fn handle(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Apply(r) => self.apply(r),
            Cmd::Register(req) => self.register(req),
        }
    }

    fn apply(&mut self, record: Record) {
        if record.stream_name != names::streams::SYSTEM
            || record.r#type != names::types::EVENTS_INDEXED
        {
            return;
        }

        self.position = record.revision;

        for key in self.collect_keys(record.revision) {
            if let Some(req) = self.inner.remove(&key) {
                let _ = req.callback.send(());
            }
        }
    }

    fn register(&mut self, request: Request) {
        if self.position >= request.position {
            let _ = request.callback.send(());
            return;
        }

        self.inner.insert(request.position, request);
    }
}

async fn indexing_subscription(
    sub_client: SubscriptionsClient,
    sender: UnboundedSender<Cmd>,
) -> eyre::Result<()> {
    let (mail, resp) = oneshot::channel();

    sub_client.subscribe(SubscribeMsg {
        payload: SubscribeTo {
            target: SubscriptionTarget::Stream(StreamTarget {
                parent: None,
                stream_name: names::streams::SYSTEM.to_string(),
            }),
        },
        mail,
    })?;

    let confirm = resp
        .await
        .wrap_err("write-request manager is unable to subscribe to $system")?;

    let mut stream = match confirm.outcome {
        SubscriptionRequestOutcome::Success(s) => s,
        SubscriptionRequestOutcome::Failure(e) => {
            tracing::error!(
                "write-request manager is unable to subscribe to $system: {}",
                e
            );

            return Err(e);
        }
    };

    while let Some(record) = stream.next().await? {
        if sender.send(Cmd::Apply(record)).is_err() {
            tracing::error!("unable to communicate with write-request manager process");
            break;
        }
    }

    tracing::warn!("write-request manager subscription to $system exited");

    Ok(())
}

async fn manager_process(mut mailbox: UnboundedReceiver<Cmd>) {
    let mut mgr = WriteRequestManager::default();

    while let Some(cmd) = mailbox.recv().await {
        mgr.handle(cmd);
    }

    tracing::warn!("write-request manager process exited")
}
