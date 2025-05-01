use std::u64;

use bytes::Bytes;
use geth_common::{ContentType, Record};
use uuid::Uuid;

use crate::process::{
    messages::{ProgramRequests, SubscribeResponses},
    subscription::{
        program::{
            pyro::{create_pyro_runtime, from_runtime_value_to_json},
            ProgramArgs,
        },
        SubscriptionClient,
    },
    Item, ProcessEnv,
};

#[tracing::instrument(skip_all, fields(proc_id = env.client.id, proc = "pyro-worker"))]
pub async fn run(mut env: ProcessEnv) -> eyre::Result<()> {
    let sub_client = SubscriptionClient::resolve(&env).await?;
    let mut args = None;

    tracing::debug!("computation unit allocated, waiting for program instructions");
    while let Some(item) = env.queue.recv().await {
        if let Item::Mail(message) = item {
            if let Some(ProgramRequests::Start { name, code, sender }) =
                message.payload.try_into().ok()
            {
                args = Some(ProgramArgs {
                    name,
                    code,
                    output: sender,
                });
                break;
            }
        }
    }

    let args = if let Some(args) = args {
        args
    } else {
        tracing::debug!("computation unit released as no program instructions were received");
        return Ok(());
    };

    let span = tracing::debug_span!("create-runtime", name = args.name).entered();
    let mut runtime = match create_pyro_runtime(sub_client, &args.name) {
        Ok(runtime) => runtime,
        Err(e) => {
            tracing::error!(error = %e, "error when creating a pyro runtime");
            let _ = args.output.send(SubscribeResponses::Error(e).into());
            return Ok(());
        }
    };
    span.exit();

    let span = tracing::debug_span!("compilation", name = args.name).entered();
    let process = match runtime.compile(&args.code) {
        Ok(process) => process,
        Err(e) => {
            tracing::error!(error = %e, "error when compiling pyro program");
            let _ = args.output.send(SubscribeResponses::Error(e).into());
            return Ok(());
        }
    };
    span.exit();

    tracing::info!(name = args.name, "ready to do work");
    let mut prog_handle = tokio::spawn(process.run());
    let mut revision = 0;

    loop {
        tokio::select! {
            outcome = &mut prog_handle => {
                if let Some(e) = outcome.err() {
                    tracing::error!(name = args.name, error = %e, "error when running pyro program");
                    let _ = args.output.send(SubscribeResponses::Error(eyre::eyre!("program panicked")).into());
                } else {
                    tracing::info!(name = args.name, "program completed successfully");
                }

                break;
            }

            Some(item) = env.queue.recv() => {
                if let Item::Mail(mail) = item {
                    if let Some(req) = mail.payload.try_into().ok() {
                        match req {
                            ProgramRequests::Stop { .. } => {
                                tracing::info!(name = args.name, "program stopped");
                                break;
                            }

                            ProgramRequests::Stats { .. } => todo!(),

                            _ => {
                                tracing::debug!("ignore program message")
                            }
                        }
                    }
                }
            }

            Some(output) = runtime.recv() => {
                // let _ = args.output.send(SubscribeResponses::Output(output).into());
                match from_runtime_value_to_json(output) {
                    Ok(json) => {
                        let resp = SubscribeResponses::Record(Record {
                            id: Uuid::new_v4(),
                            content_type: ContentType::Json,
                            class: "event-emitted".to_string(),
                            stream_name: args.name.clone(),
                            revision,
                            data: Bytes::from(serde_json::to_vec(&json)?),
                            position: u64::MAX,
                        });

                        revision += 1;
                        if args.output.send(resp.into()).is_err() {
                            tracing::warn!("exiting program because nothing is listening");
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "error when converting runtime value to JSON");
                        let _ = args.output.send(SubscribeResponses::Error(e).into());
                        break;
                    }
                }
            }

            else => {
                tracing::debug!(name = args.name, "shutting down per server request");
                prog_handle.abort();
                break;
            }
        }
    }

    Ok(())
}
