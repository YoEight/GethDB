use std::time::Instant;

use geth_common::ProgramSummary;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot,
};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    process::{
        manager::{ManagerCommand, ShutdownNotification},
        messages::Messages,
        subscription::SubscriptionClient,
        Item, Mail, ProcId, SpawnResult, Stream,
    },
    IndexClient, Proc, ReaderClient, RequestContext, WriterClient,
};

#[derive(Clone, Debug)]
pub struct ManagerClient {
    id: ProcId,
    origin_proc: Proc,
    inner: UnboundedSender<ManagerCommand>,
    shutdown_notif: ShutdownNotification,
}

impl ManagerClient {
    pub fn new_root_client(
        shutdown_notif: ShutdownNotification,
    ) -> (Self, UnboundedReceiver<ManagerCommand>) {
        let (sender, queue) = unbounded_channel();
        let client = ManagerClient {
            id: 0,
            origin_proc: Proc::Root,
            inner: sender,
            shutdown_notif,
        };

        (client, queue)
    }

    pub fn new_with_overrides(&self, id: ProcId, proc: Proc) -> Self {
        let mut temp = self.clone();
        temp.id = id;
        temp.origin_proc = proc;

        temp
    }

    pub fn id(&self) -> ProcId {
        self.id
    }

    pub async fn find(&self, proc: Proc) -> eyre::Result<Option<ProgramSummary>> {
        let (resp, receiver) = oneshot::channel();
        if self.shutdown_notif.is_shutdown()
            || self
                .inner
                .send(ManagerCommand::Find { proc, resp })
                .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        match receiver.await {
            Ok(ps) => Ok(ps),
            Err(_) => eyre::bail!("process manager has shutdown"),
        }
    }

    pub fn send(
        &self,
        context: RequestContext,
        dest: ProcId,
        payload: Messages,
    ) -> eyre::Result<()> {
        self.send_with_correlation(context, dest, Uuid::new_v4(), payload)
    }

    pub fn send_with_correlation(
        &self,
        context: RequestContext,
        dest: ProcId,
        correlation: Uuid,
        payload: Messages,
    ) -> eyre::Result<()> {
        if self.shutdown_notif.is_shutdown()
            || self
                .inner
                .send(ManagerCommand::Send {
                    dest,
                    item: Item::Mail(Mail {
                        context,
                        origin: self.id,
                        correlation,
                        payload,
                        created: Instant::now(),
                    }),
                    resp: None,
                })
                .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        Ok(())
    }

    pub async fn request_opt(
        &self,
        context: RequestContext,
        dest: ProcId,
        payload: Messages,
    ) -> eyre::Result<Option<Mail>> {
        let (resp, receiver) = oneshot::channel();
        if self.shutdown_notif.is_shutdown()
            || self
                .inner
                .send(ManagerCommand::Send {
                    dest,
                    item: Item::Mail(Mail {
                        context,
                        origin: self.id,
                        correlation: Uuid::new_v4(),
                        payload,
                        created: Instant::now(),
                    }),
                    resp: Some(resp),
                })
                .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        Ok(receiver.await.ok())
    }

    pub async fn request(
        &self,
        context: RequestContext,
        dest: ProcId,
        payload: Messages,
    ) -> eyre::Result<Mail> {
        if let Some(mail) = self.request_opt(context, dest, payload).await? {
            Ok(mail)
        } else {
            eyre::bail!("process {} doesn't exist or is down", dest)
        }
    }

    pub async fn request_stream(
        &self,
        context: RequestContext,
        dest: ProcId,
        payload: Messages,
    ) -> eyre::Result<UnboundedReceiver<Messages>> {
        let (sender, receiver) = unbounded_channel();
        if self.shutdown_notif.is_shutdown()
            || self
                .inner
                .send(ManagerCommand::Send {
                    dest,
                    item: Item::Stream(Stream {
                        context,
                        correlation: Uuid::new_v4(),
                        payload,
                        sender,
                    }),
                    resp: None,
                })
                .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        Ok(receiver)
    }

    pub fn reply(
        &self,
        context: RequestContext,
        dest: ProcId,
        correlation: Uuid,
        payload: Messages,
    ) -> eyre::Result<()> {
        if self.shutdown_notif.is_shutdown()
            || self
                .inner
                .send(ManagerCommand::Send {
                    dest,
                    item: Item::Mail(Mail {
                        context,
                        origin: self.id,
                        correlation,
                        payload,
                        created: Instant::now(),
                    }),
                    resp: None,
                })
                .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        Ok(())
    }

    #[instrument(skip(self), fields(origin = ?self.origin_proc))]
    pub async fn wait_for(&self, proc: Proc) -> eyre::Result<SpawnResult> {
        let (resp, receiver) = oneshot::channel();
        if self.shutdown_notif.is_shutdown()
            || self
                .inner
                .send(ManagerCommand::WaitFor {
                    origin: self.id,
                    proc,
                    resp,
                })
                .is_err()
        {
            eyre::bail!("process manager has shutdown");
        }

        tracing::debug!(proc = ?proc, "waiting for process to be available...");

        match receiver.await {
            Ok(res) => {
                if let Some(id) = res.ok() {
                    tracing::info!(proc = ?proc, id = %id, "process resolved");
                }

                Ok(res)
            }
            Err(_) => eyre::bail!("process manager has shutdown"),
        }
    }

    pub async fn new_writer_client(&self) -> eyre::Result<WriterClient> {
        let id = self.wait_for(Proc::Writing).await?.must_succeed()?;
        Ok(WriterClient::new(id, self.clone()))
    }

    pub async fn new_subscription_client(&self) -> eyre::Result<SubscriptionClient> {
        let id = self.wait_for(Proc::PubSub).await?.must_succeed()?;
        Ok(SubscriptionClient::new(id, self.clone()))
    }

    pub async fn new_index_client(&self) -> eyre::Result<IndexClient> {
        let id = self.wait_for(Proc::Indexing).await?.must_succeed()?;
        Ok(IndexClient::new(id, self.clone()))
    }

    pub async fn new_reader_client(&self) -> eyre::Result<ReaderClient> {
        let id = self.wait_for(Proc::Reading).await?.must_succeed()?;
        Ok(ReaderClient::new(id, self.clone()))
    }

    pub async fn shutdown(&self) -> eyre::Result<()> {
        let (resp, recv) = oneshot::channel();
        if self.inner.send(ManagerCommand::Shutdown { resp }).is_err() {
            return Ok(());
        }

        let _ = recv.await;
        Ok(())
    }

    pub async fn manager_exited(self) {
        self.shutdown_notif.wait_for_shutdown().await
    }
}
