pub use crate::options::Options;

mod domain;
mod names;
mod options;
mod process;

pub use process::{
    indexing::IndexClient,
    reading::{self, ReaderClient},
    start_process_manager, start_process_manager_with_catalog,
    writing::WriterClient,
    Catalog, CatalogBuilder, ManagerClient, Proc,
};

pub async fn run(options: Options) -> eyre::Result<()> {
    let manager = start_process_manager(options).await?;

    manager.wait_for(Proc::Grpc).await?;
    manager.manager_exited().await;

    Ok(())
}
