use std::collections::HashSet;

use bytes::Bytes;
use geth_common::{ContentType, ProgramStats, Record};
use uuid::Uuid;

use crate::{
    RequestContext,
    process::{
        Item, Managed, ProcId, ProcessEnv,
        messages::{ProgramRequests, ProgramResponses, SubscribeResponses},
        subscription::{
            program::{
                ProgramArgs,
                pyro::{create_pyro_runtime, from_runtime_value_to_json},
            },
            pyro::{PyroEvent, PyroRuntimeNotification},
        },
    },
};

struct WorkerArgs {
    context: RequestContext,
    program: ProgramArgs,
    origin: ProcId,
    correlation: Uuid,
}

#[tracing::instrument(skip_all, fields(proc_id = env.client.id(), proc = ?env.proc))]
pub async fn run(mut env: ProcessEnv<Managed>) -> eyre::Result<()> {
    let mut args = None;

    tracing::debug!("computation unit allocated, waiting for program instructions");
    while let Some(item) = env.recv().await {
        if let Item::Mail(message) = item
            && let Ok(ProgramRequests::Start { name, code, sender }) = message.payload.try_into()
        {
            args = Some(WorkerArgs {
                context: message.context,
                program: ProgramArgs {
                    name,
                    code,
                    output: sender,
                },
                origin: message.origin,
                correlation: message.correlation,
            });

            break;
        }
    }

    let args = if let Some(args) = args {
        args
    } else {
        tracing::debug!("computation unit released as no program instructions were received");
        return Ok(());
    };

    let span = tracing::debug_span!(
        "create-runtime",
        name = args.program.name,
        correlation = %args.context.correlation
    )
    .entered();
    let mut runtime = match create_pyro_runtime(
        args.context,
        env.client.clone(),
        env.client.id(),
        &args.program.name,
    ) {
        Ok(runtime) => runtime,
        Err(e) => {
            tracing::error!(correlation = %args.context.correlation, error = %e, "error when creating a pyro runtime");
            let _ = args
                .program
                .output
                .send(SubscribeResponses::Error(e).into());
            return Ok(());
        }
    };
    span.exit();

    let span =
        tracing::debug_span!("compilation", name = args.program.name, correlation = %args.context.correlation)
            .entered();
    let process = match runtime.compile(&args.program.code) {
        Ok(process) => process,
        Err(e) => {
            tracing::error!(error = %e, correlation = %args.context.correlation, "error when compiling pyro program");
            let _ = args
                .program
                .output
                .send(SubscribeResponses::Error(e).into());
            return Ok(());
        }
    };
    span.exit();

    env.client.reply(
        args.context,
        args.origin,
        args.correlation,
        ProgramResponses::Started.into(),
    )?;

    tracing::info!(name = args.program.name, correlation = %args.context.correlation, "ready to do work");
    let mut execution = Box::pin(process.run());
    let mut revision = 0;
    let mut subs = HashSet::new();

    loop {
        tokio::select! {
            outcome = &mut execution => {
                if let Err(e) = outcome {
                    tracing::error!(name = args.program.name, error = %e, correlation = %args.context.correlation, "error when running pyro program");
                    let _ = args.program.output.send(SubscribeResponses::Error(eyre::eyre!("program panicked")).into());
                } else {
                    tracing::info!(name = args.program.name, correlation = %args.context.correlation, "program completed successfully");
                }

                break;
            }

            Some(item) = env.recv() => {
                if let Item::Mail(mail) = item
                    && let Ok(req) = mail.payload.try_into() {
                        match req {
                            ProgramRequests::Stop { .. } => {
                                tracing::info!(name = args.program.name, correlation = %args.context.correlation, "program stopped");
                                let _ = env.client.reply(args.context, mail.origin, mail.correlation, ProgramResponses::Stopped.into());
                                break;
                            }

                            ProgramRequests::Stats { .. } => {
                                let _ = env.client.reply(args.context, mail.origin, mail.correlation, ProgramResponses::Stats(ProgramStats {
                                    id: env.client.id(),
                                    name: args.program.name.clone(),
                                    source_code: args.program.code.clone(),
                                    subscriptions: subs.iter().cloned().collect(),
                                    pushed_events: revision as usize,
                                    started: runtime.started(),
                                }).into());
                            }

                            x => {
                                tracing::warn!(msg = ?x, correlation = %args.context.correlation, "ignore program message")
                            }
                        }
                    }

            }

            Some(output) = runtime.recv() => {
                match output {
                    PyroEvent::Value(output) => {
                        match from_runtime_value_to_json(output) {
                            Ok(json) => {
                                let resp = SubscribeResponses::Record(Record {
                                    id: Uuid::new_v4(),
                                    content_type: ContentType::Json,
                                    class: "event-emitted".to_string(),
                                    stream_name: args.program.name.clone(),
                                    revision,
                                    data: Bytes::from(serde_json::to_vec(&json)?),
                                    position: u64::MAX,
                                });

                                revision += 1;

                                if args.program.output.send(resp.into()).is_err() {
                                    tracing::warn!(
                                        correlation = %args.context.correlation,
                                        "exiting program because nothing is listening",
                                    );

                                    break;
                                }

                                tracing::debug!(
                                    name = args.program.name,
                                    id = env.client.id(),
                                    revision = revision,
                                    correlation = %args.context.correlation,
                                    "program emitted event",
                                );
                            }

                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    correlation = %args.context.correlation,
                                    "error when converting runtime value to JSON",
                                );

                                let _ = args.program.output.send(SubscribeResponses::Error(e).into());
                                break;
                            }
                        }
                    }

                    PyroEvent::Notification(notif) => {
                        match notif {
                            PyroRuntimeNotification::SubscribedToStream(s) => {
                                subs.insert(s.clone());

                                let _ = args.program.output.send(
                                    SubscribeResponses::Programs(ProgramResponses::Subscribed(s)).into());
                            }

                            PyroRuntimeNotification::UnsubscribedToStream(s) => {
                                subs.remove(&s);

                                let _ = args.program.output.send(
                                    SubscribeResponses::Programs(ProgramResponses::Unsubscribed(s)).into());
                            }
                        }
                    }
                }
            }

            else => {
                tracing::debug!(
                    name = args.program.name,
                    correlation = %args.context.correlation,
                    "shutting down per server request",
                );

                break;
            }
        }
    }

    Ok(())
}
