mod programmable;

use crate::bus::{
    GetProgrammableSubscriptionStatsMsg, KillProgrammableSubscriptionMsg, SubscribeMsg,
};
use crate::messages::{SubscriptionConfirmed, SubscriptionRequestOutcome, SubscriptionTarget};
use geth_common::{ProgrammableStats, ProgrammableSummary};
use geth_mikoshi::{Entry, MikoshiStream};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

enum Msg {
    Subscribe(SubscribeMsg),
    EventCommitted(Entry),
    GetProgrammableSubStats(Uuid, oneshot::Sender<Option<ProgrammableStats>>),
    KillProgrammableSub(Uuid, oneshot::Sender<()>),
    ListProgrammableSubs(oneshot::Sender<Vec<ProgrammableSummary>>),
}

struct ProgrammableProcess {
    stats: ProgrammableStats,
    handle: JoinHandle<()>,
}

#[derive(Clone)]
pub struct SubscriptionsClient {
    inner: mpsc::UnboundedSender<Msg>,
}

impl SubscriptionsClient {
    pub fn subscribe(&self, msg: SubscribeMsg) -> eyre::Result<()> {
        if self.inner.send(Msg::Subscribe(msg)).is_err() {
            eyre::bail!("Subscription service process has shutdown!");
        }

        Ok(())
    }

    pub fn event_committed(&self, event: Entry) -> eyre::Result<()> {
        if self.inner.send(Msg::EventCommitted(event)).is_err() {
            eyre::bail!("Subscription service process has shutdown!");
        }

        Ok(())
    }

    pub async fn get_programmable_subscription_stats(
        &self,
        msg: GetProgrammableSubscriptionStatsMsg,
    ) -> eyre::Result<()> {
        if self
            .inner
            .send(Msg::GetProgrammableSubStats(msg.id, msg.mail))
            .is_err()
        {
            eyre::bail!("Subscription service process has shutdown!");
        }

        Ok(())
    }

    pub async fn kill_programmable_subscription(
        &self,
        msg: KillProgrammableSubscriptionMsg,
    ) -> eyre::Result<()> {
        if self
            .inner
            .send(Msg::KillProgrammableSub(msg.id, msg.mail))
            .is_err()
        {
            eyre::bail!("Subscription service process has shutdown!");
        }

        Ok(())
    }

    pub async fn list_programmable_subscriptions(
        &self,
        mailbox: oneshot::Sender<Vec<ProgrammableSummary>>,
    ) -> eyre::Result<()> {
        if self.inner.send(Msg::ListProgrammableSubs(mailbox)).is_err() {
            eyre::bail!("Subscription service process has shutdown!");
        }

        Ok(())
    }
}

pub fn start() -> SubscriptionsClient {
    let (sender, mailbox) = mpsc::unbounded_channel();
    let client = SubscriptionsClient { inner: sender };

    let local_client = client.clone();
    tokio::spawn(service(local_client, mailbox));

    client
}

struct Sub {
    parent: Option<Uuid>,
    stream: String,
    sender: mpsc::Sender<Entry>,
}

async fn service(client: SubscriptionsClient, mut mailbox: mpsc::UnboundedReceiver<Msg>) {
    let mut registry = HashMap::<String, Vec<Sub>>::new();
    let mut programmables = HashMap::<Uuid, ProgrammableProcess>::new();

    while let Some(msg) = mailbox.recv().await {
        match msg {
            Msg::Subscribe(msg) => match msg.payload.target {
                SubscriptionTarget::Stream(opts) => {
                    let subs = registry.entry(opts.stream_name.clone()).or_default();
                    let (sender, reader) = mpsc::channel(500);

                    if let Some(parent) = opts.parent {
                        if let Some(process) = programmables.get_mut(&parent) {
                            process.stats.subscriptions.push(opts.stream_name.clone());
                        }
                    }

                    subs.push(Sub {
                        parent: opts.parent,
                        stream: opts.stream_name,
                        sender,
                    });

                    let _ = msg.mail.send(SubscriptionConfirmed {
                        correlation: Uuid::new_v4(),
                        outcome: SubscriptionRequestOutcome::Success(MikoshiStream::new(reader)),
                    });
                }

                SubscriptionTarget::Process(opts) => {
                    match programmable::spawn(
                        client.clone(),
                        opts.id,
                        opts.name.clone(),
                        opts.source_code.clone(),
                    ) {
                        Ok(prog) => {
                            programmables.insert(
                                opts.id,
                                ProgrammableProcess {
                                    stats: ProgrammableStats::new(
                                        opts.id,
                                        opts.name.clone(),
                                        opts.source_code,
                                    ),
                                    handle: prog.handle,
                                },
                            );

                            let _ = msg.mail.send(SubscriptionConfirmed {
                                correlation: Uuid::new_v4(),
                                outcome: SubscriptionRequestOutcome::Success(prog.stream),
                            });
                        }

                        Err(e) => {
                            tracing::error!(
                                "Error when starting programmable subscription '{}': {}",
                                opts.name,
                                e
                            );

                            let _ = msg.mail.send(SubscriptionConfirmed {
                                correlation: Uuid::new_v4(),
                                outcome: SubscriptionRequestOutcome::Failure(eyre::eyre!(
                                    "Error when starting programmable subscription '{}': {}",
                                    opts.name,
                                    e
                                )),
                            });
                        }
                    }
                }
            },

            Msg::EventCommitted(entry) => {
                if let Some(subs) = registry.get(&entry.stream_name) {
                    for sub in subs {
                        if sub.stream != entry.stream_name {
                            continue;
                        }

                        if let Some(parent) = sub.parent {
                            if let Some(process) = programmables.get_mut(&parent) {
                                process.stats.pushed_events += 1;
                            }
                        }

                        if let Err(_) = sub.sender.send(entry.clone()).await {
                            // TODO - implement unsubscription logic here.
                        }
                    }
                }
            }

            Msg::GetProgrammableSubStats(id, mailbox) => {
                let stats = programmables.get(&id).map(|p| p.stats.clone());

                let _ = mailbox.send(stats);
            }
            Msg::KillProgrammableSub(id, mailbox) => {
                if let Some(process) = programmables.remove(&id) {
                    process.handle.abort();
                }

                let _ = mailbox.send(());
            }

            Msg::ListProgrammableSubs(mailbox) => {
                let mut summaries = Vec::with_capacity(programmables.len());

                for process in programmables.values() {
                    summaries.push(ProgrammableSummary {
                        id: process.stats.id,
                        name: process.stats.name.clone(),
                        started: process.stats.started,
                    });
                }

                let _ = mailbox.send(summaries);
            }
        }
    }

    tracing::info!("storage service exited");
}
