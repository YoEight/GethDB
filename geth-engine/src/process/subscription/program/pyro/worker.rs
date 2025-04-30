use crate::process::{
    messages::{ProgramRequests, SubscribeResponses},
    subscription::{
        program::{pyro::create_pyro_runtime, ProgramArgs},
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
                    // implement supporting incoming requests like stats.
                }
            }

            Some(output) = runtime.recv() => {
                // let _ = args.output.send(SubscribeResponses::Output(output).into());
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
