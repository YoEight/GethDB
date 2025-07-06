use crate::metrics::{get_metrics, Metrics};
use crate::names::types::STREAM_DELETED;
use crate::process::messages::{
    Messages, Notifications, ProgramProcess, ProgramRequests, ProgramResponses, Responses,
    SubscribeInternal, SubscribeRequests, SubscribeResponses, SubscriptionType,
};
use crate::process::subscription::program::{ProgramClient, ProgramStartResult};
use crate::process::{Item, Managed, ProcId, ProcessEnv};
use crate::{ManagerClient, Proc, RequestContext};
use chrono::Utc;
use geth_common::{ProgramSummary, Record};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

const ALL_IDENT: &str = "$all";

#[derive(Default)]
struct Register {
    inner: HashMap<String, Vec<UnboundedSender<Messages>>>,
}

impl Register {
    fn register(&mut self, key: String, sender: UnboundedSender<Messages>) {
        self.inner.entry(key).or_default().push(sender);
    }

    fn publish(&mut self, metrics: &Metrics, record: Record) {
        if let Some(senders) = self.inner.get_mut(&record.stream_name) {
            let before = senders.len();
            senders.retain(|sender| {
                sender
                    .send(SubscribeResponses::Record(record.clone()).into())
                    .is_ok()
                    && record.class != STREAM_DELETED
            });
            let after = senders.len();
            metrics.observe_subscription_terminated(before - after);
        }

        if let Some(senders) = self.inner.get_mut(ALL_IDENT) {
            let before = senders.len();
            senders.retain(|sender| {
                sender
                    .send(SubscribeResponses::Record(record.clone()).into())
                    .is_ok()
            });
            let after = senders.len();
            metrics.observe_subscription_terminated(before - after);
        }
    }
}

fn unit() -> eyre::Result<()> {
    Ok(())
}

struct StartPyroWorker {
    context: RequestContext,
    client: ManagerClient,
    sender: UnboundedSender<Messages>,
    name: String,
    code: String,
}

fn start_pyro_worker(args: StartPyroWorker) {
    tokio::spawn(async move {
        let result = args.client.wait_for(Proc::PyroWorker).await?;
        let id = match result.must_succeed() {
            Err(e) => {
                tracing::error!(error = %e, correlation = %args.context.correlation, "error when spawning a pyro worker");

                let _ = args.sender.send(SubscribeResponses::Error(e).into());
                return unit();
            }

            Ok(id) => id,
        };

        let client = ProgramClient::new(id, args.client.clone());

        tracing::debug!(
            id = %id,
            name = args.name,
            correlation = %args.context.correlation,
            "program is starting"
        );

        match client
            .start(
                args.context,
                args.name.clone(),
                args.code,
                args.sender.clone(),
            )
            .await?
        {
            ProgramStartResult::Started => {
                tracing::debug!(
                    id = %id,
                    name = args.name,
                    correlation = %args.context.correlation,
                    "program has started successfully"
                );

                args.client.send_to_self(
                    args.context,
                    SubscribeResponses::Internal(SubscribeInternal::ProgramStarted(
                        ProgramProcess {
                            client,
                            name: args.name,
                            sender: args.sender,
                            started_at: Utc::now(),
                        },
                    ))
                    .into(),
                )?;
            }

            ProgramStartResult::Failed(e) => {
                tracing::error!(id = %id, name = args.name, error = %e, "error when starting program");
                let _ = args.sender.send(SubscribeResponses::Error(e).into());
            }
        };

        unit()
    });
}

struct PyroWorkerStats {
    context: RequestContext,
    correlation: Uuid,
    client: ManagerClient,
    prog: ProgramClient,
    origin: ProcId,
    timeout: Duration,
}

fn spawn_pyro_worker_stats(args: PyroWorkerStats) {
    tokio::spawn(pyro_worker_stats(args));
}

#[tracing::instrument(skip_all, fields(parent_proc_id = args.client.id(), prog_id = args.prog.id(), correlation = %args.correlation))]
async fn pyro_worker_stats(args: PyroWorkerStats) -> eyre::Result<()> {
    match tokio::time::timeout(args.timeout, args.prog.stats(args.context)).await {
        Err(_) => {
            tracing::error!(deadline = ?args.timeout, "retrieving stats timeout out");

            args.client.reply(
                args.context,
                args.origin,
                args.correlation,
                SubscribeResponses::Programs(ProgramResponses::Error(eyre::eyre!("timed out")))
                    .into(),
            )?
        }

        Ok(stats) => match stats {
            Err(e) => {
                tracing::error!(error = %e, "unexpected erorr when retrieving program stats");

                args.client.reply(
                    args.context,
                    args.origin,
                    args.correlation,
                    SubscribeResponses::Programs(ProgramResponses::Error(e)).into(),
                )?
            }

            Ok(stats) => {
                if let Some(stats) = stats {
                    args.client.reply(
                        args.context,
                        args.origin,
                        args.correlation,
                        SubscribeResponses::Programs(ProgramResponses::Stats(stats)).into(),
                    )?;
                } else {
                    args.client.reply(
                        args.context,
                        args.origin,
                        args.correlation,
                        SubscribeResponses::Programs(ProgramResponses::NotFound).into(),
                    )?
                }
            }
        },
    }

    Ok(())
}

#[tracing::instrument(skip_all, fields(proc_id = env.client.id(), proc = ?env.proc))]
pub async fn run(mut env: ProcessEnv<Managed>) -> eyre::Result<()> {
    let mut reg = Register::default();
    let mut programs = HashMap::<ProcId, ProgramProcess>::new();
    let metrics = get_metrics();

    while let Some(item) = env.recv().await {
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
                                    metrics.observe_subscription_new();
                                    continue;
                                }

                                tracing::warn!(stream = ident, correlation = %stream.context.correlation, "subscription wasn't registered because nothing is listening to it");
                            }

                            SubscriptionType::Program { name, code } => {
                                start_pyro_worker(StartPyroWorker {
                                    context: stream.context,
                                    client: env.client.clone(),
                                    sender: stream.sender,
                                    name,
                                    code,
                                });
                            }
                        },

                        _ => {
                            tracing::warn!(
                                correlation = %stream.context.correlation,
                                "unsupported subscription streaming request",
                            );
                        }
                    }

                    continue;
                }

                tracing::warn!(
                    correlation = %stream.context.correlation,
                    "malformed reader request from stream request",
                );
            }

            Item::Mail(mail) => {
                if let Messages::Notifications(Notifications::ProcessTerminated(proc_id)) =
                    mail.payload
                {
                    if let Some(prog) = programs.remove(&proc_id) {
                        tracing::info!(id = proc_id, name = prog.name, "program terminated");
                        let _ = prog.sender.send(SubscribeResponses::Unsubscribed.into());
                        metrics.observe_program_terminated();
                    }

                    continue;
                }

                if let Messages::Responses(Responses::Subscribe(SubscribeResponses::Internal(
                    internal,
                ))) = mail.payload
                {
                    match internal {
                        SubscribeInternal::ProgramStarted(args) => {
                            let program_id = args.client.id();
                            let program_client = args.client.clone();

                            if args
                                .sender
                                .send(SubscribeResponses::Confirmed(Some(args.client.id())).into())
                                .is_ok()
                            {
                                tracing::debug!(name = args.name, correlation = %mail.context.correlation, "program was registered successfully");
                                programs.insert(args.client.id(), args);
                                metrics.observe_program_new();

                                continue;
                            }

                            tokio::spawn(program_client.stop(mail.context));
                            tracing::warn!(id = %program_id,  name = args.name, correlation = %mail.context.correlation, "program wasn't registered because nothing is listening to it");
                        }
                    }

                    continue;
                }

                if let Ok(req) = mail.payload.try_into() {
                    match req {
                        SubscribeRequests::Push { events } => {
                            // We don't really to confirm to the entity that sent us the push request to deliver those events first.
                            env.client.reply(
                                mail.context,
                                mail.origin,
                                mail.correlation,
                                SubscribeResponses::Pushed.into(),
                            )?;

                            for event in events {
                                reg.publish(&metrics, event);
                            }
                        }

                        SubscribeRequests::Program(req) => match req {
                            ProgramRequests::Stats { id } => {
                                if let Some(prog) = programs.get(&id) {
                                    spawn_pyro_worker_stats(PyroWorkerStats {
                                        context: mail.context,
                                        correlation: mail.correlation,
                                        client: env.client.clone(),
                                        prog: prog.client.clone(),
                                        origin: mail.origin,
                                        timeout: Duration::from_secs(5),
                                    });

                                    continue;
                                }

                                env.client.reply(
                                    mail.context,
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
                                    mail.context,
                                    mail.origin,
                                    mail.correlation,
                                    SubscribeResponses::Programs(ProgramResponses::List(summaries))
                                        .into(),
                                )?;
                            }

                            ProgramRequests::Stop { id } => {
                                if let Some(prog) = programs.remove(&id) {
                                    let client = env.client.clone();
                                    tokio::spawn(async move {
                                        let _ = tokio::time::timeout(
                                            Duration::from_secs(5),
                                            prog.client.stop(mail.context),
                                        )
                                        .await;

                                        let _ = client.reply(
                                            mail.context,
                                            mail.origin,
                                            mail.correlation,
                                            SubscribeResponses::Programs(ProgramResponses::Stopped)
                                                .into(),
                                        );
                                    });

                                    continue;
                                }

                                env.client.reply(
                                    mail.context,
                                    mail.origin,
                                    mail.correlation,
                                    SubscribeResponses::Programs(ProgramResponses::Stopped).into(),
                                )?;
                            }

                            _ => {
                                tracing::warn!(correlation = %mail.context.correlation, request = %mail.correlation, "unsupported program request");
                            }
                        },

                        _ => {
                            tracing::warn!(correlation = %mail.context.correlation, request = %mail.correlation, "unsupported subscription request");
                        }
                    }
                    continue;
                }
            }
        }
    }

    Ok(())
}
