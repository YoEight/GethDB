use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
    process::manager::{
        catalog::Catalog, client::ManagerClient, Manager, ManagerCommand, ShutdownReporter,
    },
    Options,
};

pub async fn process_manager(
    options: Options,
    client: ManagerClient,
    catalog: Catalog,
    reporter: ShutdownReporter,
    mut queue: UnboundedReceiver<ManagerCommand>,
) {
    let mut manager = Manager {
        options,
        client,
        catalog,
        proc_id_gen: 1,
        requests: Default::default(),
        closing: false,
        close_resp: vec![],
        processes_shutting_down: Default::default(),
        reporter,
    };

    tokio::spawn(async move {
        while let Some(cmd) = queue.recv().await {
            let outcome = match cmd {
                ManagerCommand::Find(cmd) => manager.handle_find(cmd),
                ManagerCommand::Send(cmd) => manager.handle_send(cmd),
                ManagerCommand::WaitFor(cmd) => manager.handle_wait_for(cmd),
                ManagerCommand::ProcTerminated(cmd) => Ok(manager.handle_terminate(cmd)),
                ManagerCommand::Shutdown(cmd) => manager.handle_shutdown(cmd),
                ManagerCommand::Timeout(cmd) => manager.handle_timeout(cmd),
            };

            if let Err(error) = outcome {
                tracing::error!(%error, "unexpected error in manager process");
                break;
            }

            if manager.client.notification().is_shutdown() {
                break;
            }
        }
    });

    // while let Some(cmd) = manager.queue.recv().await {
    //     if let Err(e) = manager.handle(cmd) {
    //         tracing::error!("unexpected: {}", e);
    //         break;
    //     }

    //     if manager.closed.load(Ordering::Acquire) {
    //         break;
    //     }
    // }

    // tracing::info!("process manager terminated");
}
