use std::sync::Arc;

use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
    process::manager::{
        catalog::Catalog, client::ManagerClient, Manager, ManagerCommand, ShutdownReporter,
    },
    Options,
};

pub fn process_manager(
    options: Options,
    client: ManagerClient,
    catalog: Catalog,
    reporter: ShutdownReporter,
    mut queue: UnboundedReceiver<ManagerCommand>,
) {
    let mut manager = Manager {
        options: Arc::new(options),
        client,
        catalog,
        requests: Default::default(),
        closing: false,
        close_resp: vec![],
        processes_shutting_down: Default::default(),
        reporter: reporter.clone(),
    };

    tokio::spawn(async move {
        while let Some(cmd) = queue.recv().await {
            let outcome = match cmd {
                ManagerCommand::Find(cmd) => manager.handle_find(cmd),
                ManagerCommand::Send(cmd) => manager.handle_send(cmd),
                ManagerCommand::WaitFor(cmd) => manager.handle_wait_for(cmd),
                ManagerCommand::Shutdown(cmd) => manager.handle_shutdown(cmd),

                ManagerCommand::ProcTerminated(cmd) => {
                    manager.handle_terminate(cmd);
                    Ok(())
                }

                ManagerCommand::Timeout(cmd) => {
                    manager.handle_timeout(cmd);
                    Ok(())
                }

                ManagerCommand::ProcReady(cmd) => {
                    manager.handle_proc_ready(cmd);
                    Ok(())
                }
            };

            if let Err(error) = outcome {
                tracing::error!(%error, "unexpected error in manager process");
                break;
            }

            if manager.client.notification().is_shutdown() {
                break;
            }
        }

        reporter.report_shutdown();
    });
}
