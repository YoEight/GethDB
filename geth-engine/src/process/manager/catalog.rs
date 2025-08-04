use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use chrono::Utc;
use geth_common::ProgramSummary;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{
    Proc, RequestContext,
    process::{Item, Mail, ProcId, RunningProc, SpawnError, SpawnResult, messages::Messages},
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
    WaitingForConfirmation(ProcId),
}

pub struct ProcIdGen {
    inner: u64,
}

impl Default for ProcIdGen {
    fn default() -> Self {
        Self { inner: 1 }
    }
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
        self.instances.len() < self.limit
    }

    fn add_instance(&mut self, id: ProcId) {
        self.instances.insert(id);
    }

    fn remove_instance(&mut self, id: ProcId) {
        self.instances.remove(&id);
    }
}

#[derive(Default)]
struct Registry {
    inner: HashMap<Proc, RegisteredProcess>,
}

impl Registry {
    fn get_process_mut(&mut self, proc: &Proc) -> Option<&mut RegisteredProcess> {
        self.inner.get_mut(proc)
    }

    fn process(&self, proc: &Proc) -> eyre::Result<&RegisteredProcess> {
        if let Some(registered) = self.inner.get(proc) {
            Ok(registered)
        } else {
            eyre::bail!("process {:?} is not registered", proc);
        }
    }

    fn process_mut(&mut self, proc: &Proc) -> eyre::Result<&mut RegisteredProcess> {
        if let Some(registered) = self.get_process_mut(proc) {
            Ok(registered)
        } else {
            eyre::bail!("process {:?} is not registered", proc);
        }
    }

    fn clear(&mut self) {
        self.inner.clear();
    }
}

impl From<HashMap<Proc, RegisteredProcess>> for Registry {
    fn from(inner: HashMap<Proc, RegisteredProcess>) -> Self {
        Self { inner }
    }
}

struct WaitingRoom {
    correlation: Uuid,
    id: ProcId,
    proc: Proc,
    attendees: Vec<oneshot::Sender<SpawnResult>>,
}

#[derive(Default)]
struct Lobby {
    inner: HashMap<ProcId, WaitingRoom>,
}

impl Lobby {
    pub fn waiting_room(&mut self, id: &ProcId) -> Option<&mut WaitingRoom> {
        self.inner.get_mut(id)
    }

    pub fn create(&mut self, room: WaitingRoom) {
        self.inner.insert(room.id, room);
    }

    pub fn remove(&mut self, id: &ProcId) -> Option<WaitingRoom> {
        self.inner.remove(id)
    }
}

#[derive(Default)]
pub struct Catalog {
    id_gen: ProcIdGen,
    registry: Registry,
    lobby: Lobby,
    monitor: HashMap<ProcId, RunningProc>,
}

impl Catalog {
    pub fn lookup(&self, proc: &Proc) -> eyre::Result<Option<ProgramSummary>> {
        let registered = self.registry.process(proc)?;
        if let Some(prev) = registered.as_singleton() {
            if let Some(run) = prev.as_ref().and_then(|p| self.monitor.get(p)) {
                Ok(Some(ProgramSummary {
                    id: run.id,
                    name: format!("{proc:?}"),
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
        let registered = self.registry.process_mut(&proc)?;

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

                if self.lobby.waiting_room(prev_id).is_some() {
                    Ok(ProvisionResult::WaitingForConfirmation(*prev_id))
                } else {
                    Ok(ProvisionResult::AlreadyProvisioned(*prev_id))
                }
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

    pub fn wait_for_confirmation(&mut self, proc_id: ProcId, resp: oneshot::Sender<SpawnResult>) {
        if let Some(room) = self.lobby.waiting_room(&proc_id) {
            room.attendees.push(resp);
            return;
        }

        tracing::warn!(proc_id, "waiting for a process that has no waiting room");
    }

    pub fn create_waiting_room(
        &mut self,
        correlation: Uuid,
        proc_id: ProcId,
        proc: Proc,
        resp: oneshot::Sender<SpawnResult>,
    ) -> eyre::Result<()> {
        if let Some(room) = self.lobby.waiting_room(&proc_id) {
            // indempotency check
            if room.correlation != correlation {
                eyre::bail!("conflicting waiting room creation for process: {}", room.id);
            }
        } else {
            self.lobby.create(WaitingRoom {
                correlation,
                id: proc_id,
                proc,
                attendees: vec![resp],
            });
        }

        Ok(())
    }

    pub fn report_process_start_timeout(&mut self, id: ProcId, correlation: Uuid) {
        if let Some(room) = self.lobby.remove(&id) {
            if room.correlation != correlation {
                return;
            }

            for attendee in room.attendees {
                let _ = attendee.send(SpawnResult::Failure {
                    proc: room.proc,
                    error: SpawnError::Timeout,
                });
            }
        }
    }

    pub fn report_process_ready(&mut self, correlation: Uuid, running: RunningProc) {
        let id = running.id;
        if let Some(room) = self.lobby.remove(&id) {
            if room.correlation != correlation {
                return;
            }

            self.monitor_process(running);

            for attendee in room.attendees {
                let _ = attendee.send(SpawnResult::Success(id));
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
            if let Some(registered) = self.registry.get_process_mut(&running.proc) {
                registered.remove_instance(id);
            }

            return Some(running);
        }

        None
    }

    pub fn processes(&self) -> impl Iterator<Item = &RunningProc> {
        self.monitor.values()
    }

    pub fn clear_running_processes(&mut self) {
        let now = Instant::now();
        self.registry.clear();

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
            registry: self.inner.into(),
            lobby: Default::default(),
            monitor: Default::default(),
        }
    }
}
