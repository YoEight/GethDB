use std::{collections::HashMap, time::Instant};

use chrono::Utc;
use geth_common::ProgramSummary;
use uuid::Uuid;

use crate::{
    process::{messages::Messages, Item, Mail, ProcId, RunningProc, Topology},
    Proc, RequestContext,
};

#[derive(Debug)]
pub enum ProvisionResult {
    AlreadyProvisioned(ProcId),
    Available(ProcId),
    LimitReached,
}

#[derive(Default)]
pub struct ProcIdGen {
    inner: u64,
}

impl ProcIdGen {
    fn next_proc_id(&mut self) -> ProcId {
        let id = self.inner;
        self.inner += 1;
        id
    }
}

#[derive(Default)]
pub struct Catalog {
    id_gen: ProcIdGen,
    inner: HashMap<Proc, Topology>,
    monitor: HashMap<ProcId, RunningProc>,
}

impl Catalog {
    pub fn lookup(&self, proc: &Proc) -> eyre::Result<Option<ProgramSummary>> {
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

    pub fn provision_process(
        &mut self,
        dependent: ProcId,
        proc: Proc,
    ) -> eyre::Result<ProvisionResult> {
        let topology = if let Some(t) = self.inner.get_mut(&proc) {
            t
        } else {
            eyre::bail!("process {:?} is not registered", proc);
        };

        match topology {
            Topology::Singleton(prev) => {
                if let Some(prev_id) = &prev {
                    // no need to track if the process that started this new process is the manager itself.
                    if dependent != 0 {
                        if let Some(running) = self.monitor.get_mut(prev_id) {
                            running.dependents.push(dependent);
                        } else {
                            eyre::bail!(
                                "running process {} was expected but is not found",
                                prev_id
                            );
                        }
                    }

                    Ok(ProvisionResult::AlreadyProvisioned(prev_id))
                } else {
                    let new_id = self.id_gen.next_proc_id();
                    *prev = Some(new_id);

                    Ok(ProvisionResult::Available(new_id))
                }
            }

            Topology::Multiple { limit, instances } => {
                if instances.len() + 1 > *limit {
                    return Ok(ProvisionResult::LimitReached);
                }

                let new_id = self.id_gen.next_proc_id();
                instances.insert(new_id);

                Ok(ProvisionResult::Available(new_id))
            }
        }
    }

    pub fn monitor_process(&mut self, running: RunningProc) {
        self.monitor.insert(running.id, running);
    }

    pub fn get_process(&self, id: ProcId) -> Option<&RunningProc> {
        self.monitor.get(&id)
    }

    pub fn get_process_mut(&mut self, id: ProcId) -> Option<&mut RunningProc> {
        self.monitor.get_mut(&id)
    }

    pub fn remove_process(&mut self, id: ProcId) -> Option<RunningProc> {
        if let Some(running) = self.monitor.remove(&id) {
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

            return Some(running);
        }

        None
    }

    pub fn processes<'a>(&'a self) -> impl Iterator<Item = &'a RunningProc> {
        self.monitor.values()
    }

    pub fn clear_running_processes(&mut self) {
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
            id_gen: Default::default(),
            inner: self.inner,
            monitor: Default::default(),
        }
    }
}
