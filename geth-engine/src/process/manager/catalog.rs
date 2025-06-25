use std::{collections::HashMap, time::Instant};

use chrono::Utc;
use geth_common::ProgramSummary;
use uuid::Uuid;

use crate::{
    process::{messages::Messages, Item, Mail, ProcId, RunningProc, Topology},
    Proc, RequestContext,
};

pub struct Catalog {
    inner: HashMap<Proc, Topology>,
    monitor: HashMap<ProcId, RunningProc>,
}

impl Catalog {
    fn lookup(&self, proc: &Proc) -> eyre::Result<Option<ProgramSummary>> {
        if let Some(topology) = self.inner.get(proc) {
            return match &topology {
                Topology::Singleton(prev) => {
                    if let Some(run) = prev.as_ref().and_then(|p| self.monitor.get(p)) {
                        Ok(Some(ProgramSummary {
                            id: run.id,
                            name: format!("{:?}", proc),
                            started_at: Utc::now(), // TODO - no use for date times.
                        }))
                    } else {
                        Ok(None)
                    }
                }

                Topology::Multiple { .. } => {
                    // It doesn't really make sense for this topology.
                    Ok(None)
                }
            };
        }

        eyre::bail!("process {:?} is not registered", proc);
    }

    fn remove(&mut self, running: &RunningProc) {
        if let Some(mut topology) = self.inner.get_mut(&running.proc) {
            match &mut topology {
                Topology::Singleton(prev) => {
                    *prev = None;
                }

                Topology::Multiple { instances, .. } => {
                    instances.remove(&running.id);
                }
            }
        }
    }

    fn clear_running_processes(&mut self) {
        let now = Instant::now();
        for (_, running) in std::mem::take(&mut self.monitor) {
            self.remove(&running);

            running.mailbox.send(Item::Mail(Mail {
                origin: 0,
                correlation: Uuid::nil(),
                context: RequestContext::nil(),
                payload: Messages::Shutdown,
                created: now,
            }));

            tracing::debug!(proc_id = running.id, proc = ?running.proc, "process cleared from the monitor and the catalog");
        }
    }
}

impl Catalog {
    pub fn builder() -> CatalogBuilder {
        CatalogBuilder {
            inner: HashMap::new(),
        }
    }
}

pub struct CatalogBuilder {
    inner: HashMap<Proc, Topology>,
}

impl CatalogBuilder {
    pub fn register(mut self, proc: Proc) -> Self {
        self.inner.insert(proc, Topology::Singleton(None));

        self
    }

    pub fn register_multiple(mut self, proc: Proc, limit: usize) -> Self {
        self.inner.insert(
            proc,
            Topology::Multiple {
                limit,
                instances: Default::default(),
            },
        );

        self
    }

    pub fn build(self) -> Catalog {
        Catalog {
            inner: self.inner,
            monitor: Default::default(),
        }
    }
}
