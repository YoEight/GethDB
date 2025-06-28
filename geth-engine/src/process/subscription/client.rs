use crate::process::messages::{
    Messages, ProgramRequests, ProgramResponses, SubscribeRequests, SubscribeResponses,
    SubscriptionType,
};
use crate::process::{ManagerClient, ProcId, RequestContext};
use geth_common::{
    ProgramStats, ProgramSummary, Record, SubscriptionConfirmation, SubscriptionEvent,
    SubscriptionNotification, UnsubscribeReason,
};
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::instrument;

#[derive(Debug)]
pub struct Streaming {
    context: RequestContext,
    stream_name: String,
    id: Option<ProcId>,
    inner: UnboundedReceiver<Messages>,
}

impl Streaming {
    pub fn from(
        context: RequestContext,
        stream_name: String,
        inner: UnboundedReceiver<Messages>,
    ) -> Self {
        Self {
            context,
            stream_name,
            inner,
            id: None,
        }
    }

    pub fn id(&self) -> ProcId {
        self.id.unwrap_or_default()
    }

    pub async fn wait_until_confirmation(&mut self) -> eyre::Result<ProcId> {
        if let Some(id) = self.id {
            return Ok(id);
        }

        if let Some(SubscriptionEvent::Confirmed(_)) = self.next().await? {
            return Ok(self.id.unwrap_or_default());
        }

        eyre::bail!("subscription never got confirmed")
    }

    pub async fn next(&mut self) -> eyre::Result<Option<SubscriptionEvent>> {
        if let Some(resp) = self.inner.recv().await.and_then(|r| r.try_into().ok()) {
            match resp {
                SubscribeResponses::Error(e) => {
                    return Err(e);
                }

                SubscribeResponses::Record(record) => {
                    return Ok(Some(SubscriptionEvent::EventAppeared(record)));
                }

                SubscribeResponses::Confirmed(proc_id) => {
                    let conf = if let Some(id) = proc_id {
                        self.id = Some(id);
                        tracing::debug!(proc_id = proc_id, correlation = %self.context.correlation, "subscription confirmed");
                        SubscriptionConfirmation::ProcessId(id)
                    } else {
                        self.id = Some(0);
                        tracing::debug!(stream_name = self.stream_name, correlation = %self.context.correlation, "subscription confirmed");
                        SubscriptionConfirmation::StreamName(std::mem::take(&mut self.stream_name))
                    };

                    return Ok(Some(SubscriptionEvent::Confirmed(conf)));
                }

                SubscribeResponses::Unsubscribed => {
                    self.inner.close();

                    // should be already empty but best to be sure.
                    while self.inner.recv().await.is_some() {}

                    return Ok(Some(SubscriptionEvent::Unsubscribed(
                        UnsubscribeReason::Server,
                    )));
                }

                SubscribeResponses::Programs(prog) if self.id.is_some() => match prog {
                    ProgramResponses::Subscribed(s) => {
                        return Ok(Some(SubscriptionEvent::Notification(
                            SubscriptionNotification::Subscribed(s),
                        )))
                    }

                    ProgramResponses::Unsubscribed(s) => {
                        return Ok(Some(SubscriptionEvent::Notification(
                            SubscriptionNotification::Unsubscribed(s),
                        )))
                    }

                    x => {
                        tracing::error!(msg = ?x, correlation = %self.context.correlation, "unexpected message");
                        eyre::bail!("unexpected message when streaming from process");
                    }
                },

                x => {
                    tracing::error!(msg = ?x, correlation = %self.context.correlation, "unexpected message");
                    eyre::bail!("unexpected message when streaming from subscription");
                }
            }
        }

        Ok(None)
    }
}

#[derive(Clone)]
pub struct SubscriptionClient {
    target: ProcId,
    inner: ManagerClient,
}

impl SubscriptionClient {
    pub fn new(target: ProcId, inner: ManagerClient) -> Self {
        Self { target, inner }
    }

    #[instrument(skip(self, context), fields(correlation = %context.correlation))]
    pub async fn subscribe_to_stream(
        &self,
        context: RequestContext,
        stream_name: &str,
    ) -> eyre::Result<Streaming> {
        let mailbox = self
            .inner
            .request_stream(
                context,
                self.target,
                SubscribeRequests::Subscribe(SubscriptionType::Stream {
                    ident: stream_name.to_string(),
                })
                .into(),
            )
            .await?;

        Ok(Streaming::from(context, stream_name.to_string(), mailbox))
    }

    #[instrument(skip_all, fields(correlation = %context.correlation))]
    pub async fn subscribe_to_program(
        &self,
        context: RequestContext,
        name: &str,
        code: &str,
    ) -> eyre::Result<Streaming> {
        let mailbox = self
            .inner
            .request_stream(
                context,
                self.target,
                SubscribeRequests::Subscribe(SubscriptionType::Program {
                    name: name.to_string(),
                    code: code.to_string(),
                })
                .into(),
            )
            .await?;

        Ok(Streaming::from(context, String::default(), mailbox))
    }

    #[instrument(skip(self, context), fields(correlation = %context.correlation))]
    pub async fn list_programs(
        &self,
        context: RequestContext,
    ) -> eyre::Result<Vec<ProgramSummary>> {
        let mailbox = self
            .inner
            .request(
                context,
                self.target,
                SubscribeRequests::Program(ProgramRequests::List).into(),
            )
            .await?;

        if let Ok(resp) = mailbox.payload.try_into() {
            match resp {
                SubscribeResponses::Error(e) => {
                    return Err(e);
                }

                SubscribeResponses::Programs(ProgramResponses::List(list)) => {
                    return Ok(list);
                }

                _ => {
                    eyre::bail!("protocol error when communicating with the pubsub process");
                }
            }
        }

        eyre::bail!("pubsub sent an unexpected response")
    }

    #[instrument(skip(self, context), fields(correlation = %context.correlation))]
    pub async fn program_stats(
        &self,
        context: RequestContext,
        id: ProcId,
    ) -> eyre::Result<Option<ProgramStats>> {
        let mailbox = self
            .inner
            .request(
                context,
                self.target,
                SubscribeRequests::Program(ProgramRequests::Stats { id }).into(),
            )
            .await?;

        if let Ok(resp) = mailbox.payload.try_into() {
            match resp {
                SubscribeResponses::Error(e) => {
                    return Err(e);
                }

                SubscribeResponses::Programs(ProgramResponses::Stats(resp)) => {
                    return Ok(Some(resp));
                }

                SubscribeResponses::Programs(ProgramResponses::NotFound) => {
                    return Ok(None);
                }

                _ => {
                    eyre::bail!("protocol error when communicating with the pubsub process");
                }
            }
        }

        eyre::bail!("pubsub process is no longer running")
    }

    #[instrument(skip(self, context), fields(correlation = %context.correlation))]
    pub async fn program_stop(&self, context: RequestContext, id: ProcId) -> eyre::Result<()> {
        let mailbox = self
            .inner
            .request(
                context,
                self.target,
                SubscribeRequests::Program(ProgramRequests::Stop { id }).into(),
            )
            .await?;

        if let Ok(resp) = mailbox.payload.try_into() {
            match resp {
                SubscribeResponses::Error(e) => {
                    return Err(e);
                }

                SubscribeResponses::Programs(ProgramResponses::Stopped) => {
                    return Ok(());
                }

                _ => {
                    eyre::bail!("protocol error when communicating with the pubsub process");
                }
            }
        }

        eyre::bail!("pubsub process is no longer running")
    }

    #[instrument(skip(self, events, context), fields(origin = ?self.inner.origin(), target = self.target, correlation = %context.correlation))]
    pub async fn push(&self, context: RequestContext, events: Vec<Record>) -> eyre::Result<()> {
        let resp = self
            .inner
            .request(
                context,
                self.target,
                SubscribeRequests::Push { events }.into(),
            )
            .await?;

        if let Ok(resp) = resp.payload.try_into() {
            match resp {
                SubscribeResponses::Error(e) => {
                    return Err(e);
                }

                SubscribeResponses::Pushed => {
                    tracing::debug!("push request confirmed by the pubsub process");
                    return Ok(());
                }

                _ => {
                    eyre::bail!("protocol error when communicating with the pubsub process");
                }
            }
        }

        eyre::bail!("unexpected response from the pubsub process")
    }
}
