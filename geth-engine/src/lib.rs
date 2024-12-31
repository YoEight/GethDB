use process::start_process_manager;
use std::path::PathBuf;

use geth_mikoshi::storage::FileSystemStorage;

pub use crate::options::Options;

mod bus;
mod domain;
mod messages;
mod names;
mod options;
mod process;

pub async fn run(options: Options) -> eyre::Result<()> {
    let storage = FileSystemStorage::new(PathBuf::from(&options.db))?;
    start_process_manager(storage).await?.manager_exited().await;

    Ok(())
}
