use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use chrono::Utc;
use geth_common::ProgramSummary;
use uuid::Uuid;

use crate::{
    process::{messages::Messages, Item, Mail, ProcId, RunningProc},
    Proc, RequestContext,
};

#[derive(Debug)]
pub struct Provision {
    pub id: ProcId,
    pub proc: Proc,
}

#[derive(Debug)]
pub enum ProvisionResult {
    AlreadyProvisioned(ProcId),
    Available(Provision),
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
struct RegisteredProcess {
    limit: usize,
    process: Proc,
    instances: HashSet<ProcId>,
}

impl RegisteredProcess {
    fn singleton(process: Proc) -> Self {
        Self {
            limit: 1,
            process,
            instances: Default::default(),
        }
    }

    fn multiple(limit: usize, process: Proc) -> Self {
        debug_assert!(limit != 0);

        Self {
            limit,
            process,
            instances: Default::default(),
        }
    }

    fn as_singleton(&self) -> Option<Option<ProcId>> {
        if self.limit != 1 {
            return None;
        }

        Some(self.instances.iter().last().copied())
    }

    fn has_capacity(&self) -> bool {
        self.instances.len() + 1 <= self.limit
    }

    fn add_instance(&mut self, id: ProcId) {
        self.instances.insert(id);
    }

    fn remove_instance(&mut self, id: ProcId) {
        self.instances.remove(&id);
    }
}

#[derive(Default)]
pub struct Catalog {
    id_gen: ProcIdGen,
    inner: HashMap<Proc, RegisteredProcess>,
    monitor: HashMap<ProcId, RunningProc>,
}

impl Catalog {
    pub fn lookup(&self, proc: &Proc) -> eyre::Result<Option<ProgramSummary>> {
        let registered = if let Some(r) = self.inner.get(proc) {
            r
        } else {
            eyre::bail!("process {:?} is not registered", proc);
        };

        if let Some(prev) = registered.as_singleton() {
            if let Some(run) = prev.as_ref().and_then(|p| self.monitor.get(p)) {
                Ok(Some(ProgramSummary {
                    id: run.id,
                    name: format!("{:?}", proc),
                    started_at: Utc::now(), // TODO - no use for date times.
                }))
            } else {
                Ok(None)
            }
        } else {
            // It doesn't really make sense for this topology.
            Ok(None)
        }
    }

    pub fn provision_process(
        &mut self,
        dependent: ProcId,
        proc: Proc,
    ) -> eyre::Result<ProvisionResult> {
        let registered = if let Some(t) = self.inner.get_mut(&proc) {
            t
        } else {
            eyre::bail!("process {:?} is not registered", proc);
        };

        if let Some(prev) = registered.as_singleton() {
            if let Some(prev_id) = &prev {
                // no need to track if the process that started this new process is the manager itself.
                if dependent != 0 {
                    if let Some(running) = self.monitor.get_mut(prev_id) {
                        running.dependents.push(dependent);
                    } else {
                        eyre::bail!("running process {} was expected but is not found", prev_id);
                    }
                }

                Ok(ProvisionResult::AlreadyProvisioned(*prev_id))
            } else {
                let new_id = self.id_gen.next_proc_id();
                registered.add_instance(new_id);

                Ok(ProvisionResult::Available(Provision {
                    id: new_id,
                    proc: registered.process,
                }))
            }
        } else {
            // TODO - improve so adding a new instance is fool proof. Right now, we need to make
            // sure that we have enough capacity first.
            if !registered.has_capacity() {
                return Ok(ProvisionResult::LimitReached);
            }

            let new_id = self.id_gen.next_proc_id();
            registered.add_instance(new_id);

            Ok(ProvisionResult::Available(Provision {
                id: new_id,
                proc: registered.process,
            }))
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
            if let Some(registered) = self.inner.get_mut(&running.proc) {
                registered.remove_instance(id);
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
        self.inner.clear();

        for (_, running) in self.monitor.drain() {
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
    inner: HashMap<Proc, RegisteredProcess>,
}

impl CatalogBuilder {
    pub fn register(mut self, process: Proc) -> Self {
        if process == Proc::Root {
            return self;
        }

        self.inner
            .insert(process, RegisteredProcess::singleton(process));

        self
    }

    pub fn register_multiple(mut self, limit: usize, process: Proc) -> Self {
        if process == Proc::Root {
            return self;
        }

        self.inner
            .insert(process, RegisteredProcess::multiple(limit, process));

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
