use std::time::{Duration, Instant};

use geth_common::ProgramSummary;
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    oneshot,
};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    IndexClient, Proc, ReaderClient, RequestContext, WriterClient,
    process::{
        Item, Mail, ProcId, RunningProc, SpawnResult, Stream,
        manager::{
            FindParams, ManagerCommand, ProcReadyParams, ProcTerminatedParams, SendParams,
            ShutdownNotification, ShutdownParams, TimeoutParams, TimeoutTarget, WaitForParams,
        },
        messages::Messages,
        subscription::SubscriptionClient,
    },
};

#[derive(Clone, Debug)]
pub struct ManagerClient {
    id: ProcId,
    origin_proc: Proc,
    inner: UnboundedSender<ManagerCommand>,
    shutdown_notif: ShutdownNotification,
}

impl ManagerClient {
    pub(crate) fn new_root_client(
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

    pub fn origin(&self) -> Proc {
        self.origin_proc
    }

    pub fn notification(&self) -> &ShutdownNotification {
        &self.shutdown_notif
    }

    fn send_internal(&self, cmd: ManagerCommand) -> eyre::Result<()> {
        if self.shutdown_notif.is_shutdown() || self.inner.send(cmd).is_err() {
            eyre::bail!("process manager has shutdown");
        }

        Ok(())
    }

    pub fn send_to_self(&self, context: RequestContext, payload: Messages) -> eyre::Result<()> {
        self.send(context, self.id, payload)
    }

    pub async fn find(&self, proc: Proc) -> eyre::Result<Option<ProgramSummary>> {
        let (resp, receiver) = oneshot::channel();

        self.send_internal(ManagerCommand::Find(FindParams { proc, resp }))?;

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
        self.send_internal(ManagerCommand::Send(SendParams {
            dest,
            item: Item::Mail(Mail {
                context,
                origin: self.id,
                correlation,
                payload,
                created: Instant::now(),
            }),
            resp: None,
        }))
    }

    pub async fn request_opt(
        &self,
        context: RequestContext,
        dest: ProcId,
        payload: Messages,
    ) -> eyre::Result<Option<Mail>> {
        let (resp, receiver) = oneshot::channel();
        self.send_internal(ManagerCommand::Send(SendParams {
            dest,
            item: Item::Mail(Mail {
                context,
                origin: self.id,
                correlation: Uuid::new_v4(),
                payload,
                created: Instant::now(),
            }),
            resp: Some(resp),
        }))?;

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
        self.send_internal(ManagerCommand::Send(SendParams {
            dest,
            item: Item::Stream(Stream {
                context,
                correlation: Uuid::new_v4(),
                payload,
                sender,
            }),
            resp: None,
        }))?;

        Ok(receiver)
    }

    pub fn reply(
        &self,
        context: RequestContext,
        dest: ProcId,
        correlation: Uuid,
        payload: Messages,
    ) -> eyre::Result<()> {
        self.send_internal(ManagerCommand::Send(SendParams {
            dest,
            item: Item::Mail(Mail {
                context,
                origin: self.id,
                correlation,
                payload,
                created: Instant::now(),
            }),
            resp: None,
        }))
    }

    #[instrument(skip(self), fields(origin = ?self.origin_proc))]
    pub async fn wait_for(&self, proc: Proc) -> eyre::Result<SpawnResult> {
        let (resp, receiver) = oneshot::channel();
        self.send_internal(ManagerCommand::WaitFor(WaitForParams {
            origin: self.id,
            proc,
            resp,
        }))?;

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

    pub fn report_process_terminated(&self, id: ProcId, error: Option<eyre::Report>) {
        let _ = self.send_internal(ManagerCommand::ProcTerminated(ProcTerminatedParams {
            id,
            error,
        }));
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

    pub(crate) fn send_timeout_in(
        &self,
        correlation: Uuid,
        target: TimeoutTarget,
        duration: Duration,
    ) {
        let this = self.clone();

        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            this.send_internal(ManagerCommand::Timeout(TimeoutParams {
                target,
                correlation,
            }))
        });
    }

    pub fn send_process_ready(&self, correlation: Uuid, running: RunningProc) {
        let _ = self.send_internal(ManagerCommand::ProcReady(ProcReadyParams {
            correlation,
            running,
        }));
    }

    pub async fn shutdown(&self) -> eyre::Result<()> {
        let (resp, recv) = oneshot::channel();
        if self
            .send_internal(ManagerCommand::Shutdown(ShutdownParams { resp }))
            .is_err()
        {
            return Ok(());
        }

        let _ = recv.await;
        Ok(())
    }

    pub async fn manager_exited(self) {
        self.shutdown_notif.wait_for_shutdown().await
    }
}
