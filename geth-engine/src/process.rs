use geth_mikoshi::wal::chunks::ChunkContainer;
use geth_mikoshi::{FileSystemStorage, InMemoryStorage};
use messages::Messages;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::process::manager::{Catalog, ManagerClient};
use crate::Options;

#[cfg(test)]
mod tests;

pub mod consumer;
#[cfg(test)]
mod echo;
mod env;
pub mod grpc;
pub mod indexing;
pub mod manager;
mod messages;
#[cfg(test)]
mod panic;
pub mod reading;
#[cfg(test)]
mod sink;
pub mod subscription;
pub mod writing;

pub use env::{Managed, ProcessEnv, Raw};

#[derive(Debug, Clone, Copy)]
pub struct RequestContext {
    pub correlation: Uuid,
}

impl RequestContext {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        RequestContext {
            correlation: Uuid::new_v4(),
        }
    }

    pub fn nil() -> Self {
        RequestContext {
            correlation: Uuid::nil(),
        }
    }
}

#[derive(Clone)]
enum Mailbox {
    Tokio(UnboundedSender<Item>),
    Raw(std::sync::mpsc::Sender<Item>),
}

impl Mailbox {
    pub fn send(&self, item: Item) -> bool {
        match self {
            Mailbox::Tokio(x) => x.send(item).is_ok(),
            Mailbox::Raw(x) => x.send(item).is_ok(),
        }
    }
}

pub type ProcId = u64;

pub struct Mail {
    pub origin: ProcId,
    pub correlation: Uuid,
    pub context: RequestContext,
    pub payload: Messages,
    pub created: Instant,
}

#[derive(Clone, Copy, PartialOrd, PartialEq, Eq, Ord, Hash, Debug)]
pub enum Proc {
    Root,
    Writing,
    Reading,
    Indexing,
    PubSub,
    Grpc,
    PyroWorker,
    #[cfg(test)]
    Echo,
    #[cfg(test)]
    Sink,
    #[cfg(test)]
    Panic,
}

enum Topology {
    Singleton(Option<ProcId>),
    Multiple {
        limit: usize,
        instances: HashSet<ProcId>,
    },
}

struct RunningProc {
    id: ProcId,
    proc: Proc,
    last_received_request: Uuid,
    mailbox: Mailbox,
    dependents: Vec<ProcId>,
}

pub struct Stream {
    context: RequestContext,
    correlation: Uuid,
    payload: Messages,
    sender: UnboundedSender<Messages>,
}

pub enum Item {
    Mail(Mail),
    Stream(Stream),
}

impl Item {
    pub fn is_shutdown(&self) -> bool {
        match self {
            Item::Mail(mail) => {
                matches!(&mail.payload, Messages::Shutdown)
            }

            Item::Stream(stream) => {
                matches!(&stream.payload, Messages::Shutdown)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpawnError {
    LimitReached,
}

pub enum SpawnResult {
    Success(ProcId),
    Failure { proc: Proc, error: SpawnError },
}

impl SpawnResult {
    pub fn ok(&self) -> Option<ProcId> {
        match self {
            SpawnResult::Success(id) => Some(*id),
            _ => None,
        }
    }

    pub fn must_succeed(self) -> eyre::Result<ProcId> {
        match self {
            SpawnResult::Success(id) => Ok(id),
            SpawnResult::Failure { proc, error } => match error {
                SpawnError::LimitReached => eyre::bail!("process {:?} limit reached", proc),
            },
        }
    }
}

pub async fn start_process_manager(options: Options) -> eyre::Result<ManagerClient> {
    let catalog = Catalog::builder()
        .register(Proc::Indexing)
        .register(Proc::Writing)
        .register(Proc::Reading)
        .register(Proc::PubSub)
        .register(Proc::Grpc)
        .register_multiple(Proc::PyroWorker, 8)
        .build();

    let client = start_process_manager_with_catalog(options, catalog).await?;

    client.wait_for(Proc::Indexing).await?;
    client.wait_for(Proc::PubSub).await?;
    client.wait_for(Proc::Reading).await?;
    client.wait_for(Proc::Writing).await?;

    Ok(client)
}

fn load_fs_chunk_container(options: &Options) -> eyre::Result<ChunkContainer<FileSystemStorage>> {
    let storage = FileSystemStorage::new(options.db.clone().into())?;
    geth_mikoshi::storage::init(&storage)?;

    let container = ChunkContainer::load(storage)?;
    Ok(container)
}

#[derive(Clone)]
pub struct Runtime<S> {
    container: ChunkContainer<S>,
}

impl<S> Runtime<S> {
    pub fn container(&self) -> &ChunkContainer<S> {
        &self.container
    }
}
