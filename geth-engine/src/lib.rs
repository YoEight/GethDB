use process::{start_process_manager, Proc};

pub use crate::options::Options;

mod bus;
mod domain;
mod messages;
mod names;
mod options;
mod process;

pub async fn run(options: Options) -> eyre::Result<()> {
    let manager = start_process_manager(options).await?;

    manager.wait_for(Proc::Grpc).await?;
    manager.manager_exited().await;

    Ok(())
}
