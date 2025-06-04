use crate::names::types::STREAM_DELETED;
use crate::process::messages::{
    Messages, ProgramRequests, ProgramResponses, SubscribeRequests, SubscribeResponses,
    SubscriptionType,
};
use crate::process::subscription::program::{ProgramClient, ProgramStartResult};
use crate::process::{Item, ProcessEnv};
use crate::Proc;
use chrono::{DateTime, Utc};
use geth_common::{ProgramSummary, Record};
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;

const ALL_IDENT: &str = "$all";

pub struct ProgramProcess {
    client: ProgramClient,
    name: String,
    started_at: DateTime<Utc>,
}

#[derive(Default)]
struct Register {
    inner: HashMap<String, Vec<UnboundedSender<Messages>>>,
}

impl Register {
    fn register(&mut self, key: String, sender: UnboundedSender<Messages>) {
        self.inner.entry(key).or_default().push(sender);
    }

    fn publish(&mut self, record: Record) {
        if let Some(senders) = self.inner.get_mut(&record.stream_name) {
            senders.retain(|sender| {
                sender
                    .send(SubscribeResponses::Record(record.clone()).into())
                    .is_ok()
                    && record.class != STREAM_DELETED
            });
        }

        if let Some(senders) = self.inner.get_mut(ALL_IDENT) {
            senders.retain(|sender| {
                sender
                    .send(SubscribeResponses::Record(record.clone()).into())
                    .is_ok()
            });
        }
    }
}

#[tracing::instrument(skip_all, fields(proc_id = env.client.id, proc = "pubsub"))]
pub async fn run(mut env: ProcessEnv) -> eyre::Result<()> {
    let mut reg = Register::default();
    let mut programs = HashMap::new();

    while let Some(item) = env.queue.recv().await {
        match item {
            Item::Stream(stream) => {
                if let Ok(req) = stream.payload.try_into() {
                    match req {
                        SubscribeRequests::Subscribe(r#type) => match r#type {
                            SubscriptionType::Stream { ident } => {
                                if stream
                                    .sender
                                    .send(SubscribeResponses::Confirmed(None).into())
                                    .is_ok()
                                {
                                    reg.register(ident, stream.sender);
                                    continue;
                                }

                                tracing::warn!(stream = ident, "subscription wasn't registered because nothing is listening to it");
                            }

                            SubscriptionType::Program { name, code } => {
                                let result = env.client.wait_for(Proc::PyroWorker).await?;
                                let id = match result.must_succeed() {
                                    Err(e) => {
                                        let _ =
                                            stream.sender.send(SubscribeResponses::Error(e).into());

                                        continue;
                                    }

                                    Ok(id) => id,
                                };

                                let client = ProgramClient::new(id, env.client.clone());

                                tracing::debug!(
                                    id = %id,
                                    name = name,
                                    "program is starting"
                                );

                                match client
                                    .start(name.clone(), code, stream.sender.clone())
                                    .await?
                                {
                                    ProgramStartResult::Started => {
                                        tracing::debug!(
                                            id = %id,
                                            name = name,
                                            "program has started successfully"
                                        );

                                        if stream
                                            .sender
                                            .send(SubscribeResponses::Confirmed(Some(id)).into())
                                            .is_ok()
                                        {
                                            programs.insert(
                                                id,
                                                ProgramProcess {
                                                    client,
                                                    name,
                                                    started_at: Utc::now(),
                                                },
                                            );
                                            continue;
                                        }

                                        client.stop().await?;
                                        tracing::warn!(id = %id,  name = name, "program wasn't registered because nothing is listening to it");
                                    }

                                    ProgramStartResult::Failed(e) => {
                                        tracing::error!(id = %id, name = name, error = %e, "error when starting program");
                                        let _ =
                                            stream.sender.send(SubscribeResponses::Error(e).into());
                                    }
                                };
                            }
                        },
                        _ => {
                            tracing::warn!(
                                correlation = stream.correlation.to_string(),
                                "unsupported subscription streaming request",
                            );
                        }
                    }

                    continue;
                }

                tracing::warn!(
                    "malformed reader request from stream request {}",
                    stream.correlation
                );
            }

            Item::Mail(mail) => {
                if let Ok(req) = mail.payload.try_into() {
                    match req {
                        SubscribeRequests::Push { events } => {
                            // We don't really to confirm to the entity that sent us the push request to deliver those events first.
                            env.client.reply(
                                mail.origin,
                                mail.correlation,
                                SubscribeResponses::Pushed.into(),
                            )?;

                            for event in events {
                                reg.publish(event);
                            }
                        }

                        SubscribeRequests::Program(req) => match req {
                            ProgramRequests::Stats { id } => {
                                if let Some(prog) = programs.get(&id) {
                                    if let Some(stats) = prog.client.stats().await? {
                                        env.client.reply(
                                            mail.origin,
                                            mail.correlation,
                                            SubscribeResponses::Programs(ProgramResponses::Stats(
                                                stats,
                                            ))
                                            .into(),
                                        )?;

                                        continue;
                                    }
                                }

                                env.client.reply(
                                    mail.origin,
                                    mail.correlation,
                                    SubscribeResponses::Programs(ProgramResponses::NotFound).into(),
                                )?;
                            }

                            ProgramRequests::List => {
                                let mut summaries = Vec::with_capacity(programs.len());

                                for prog in programs.values() {
                                    summaries.push(ProgramSummary {
                                        id: prog.client.id(),
                                        name: prog.name.clone(),
                                        started_at: prog.started_at,
                                    });
                                }

                                env.client.reply(
                                    mail.origin,
                                    mail.correlation,
                                    SubscribeResponses::Programs(ProgramResponses::List(summaries))
                                        .into(),
                                )?;
                            }

                            ProgramRequests::Stop { id } => {
                                if let Some(prog) = programs.remove(&id) {
                                    prog.client.stop().await?;
                                }

                                env.client.reply(
                                    mail.origin,
                                    mail.correlation,
                                    SubscribeResponses::Programs(ProgramResponses::Stopped).into(),
                                )?;
                            }

                            _ => {
                                tracing::warn!("unsupported program request {}", mail.correlation);
                            }
                        },

                        _ => {
                            tracing::warn!("unsupported subscription request {}", mail.correlation);
                        }
                    }
                    continue;
                }
            }
        }
    }

    Ok(())
}
