use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::io;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use rand::{thread_rng, Rng};

use crate::entry::{Entry, EntryId};
use crate::msg::{
    AppendEntries, EntriesAppended, EntriesReplicated, RequestVote, VoteCasted, VoteReceived,
};
use crate::state_machine::RaftSM;

mod entry;
mod msg;

mod state_machine;
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub enum Msg<Id, Command> {
    RequestVote(RequestVote<Id>),
    AppendEntries(AppendEntries<Id>),
    VoteReceived(VoteReceived<Id>),
    EntriesAppended(EntriesAppended<Id>),
    Command(Command),
    Tick,
    Shutdown,
}

#[derive(Debug)]
pub enum Request<Id> {
    RequestVote(RequestVote<Id>),
    AppendEntries(AppendEntries<Id>),
    VoteCasted(VoteCasted<Id>),
    EntriesReplicated(EntriesReplicated<Id>),
}

pub trait RaftCommand {
    fn write(&self, buffer: &mut BytesMut);
}

pub trait CommandDispatch {
    type Command: UserCommand;

    fn dispatch(&self, cmd: Self::Command);
}

pub trait UserCommand: RaftCommand {
    fn is_read(&self) -> bool;

    fn reject(self);
}

pub trait RaftRecv {
    type Id: Ord;
    type Command: UserCommand;

    fn recv(&mut self) -> Option<Msg<Self::Id, Self::Command>>;
}

pub trait RaftSender {
    type Id: Ord;

    fn send(&self, target: Self::Id, request: Request<Self::Id>);

    fn request_vote(&self, target: Self::Id, req: RequestVote<Self::Id>) {
        self.send(target, Request::RequestVote(req));
    }

    fn vote_casted(&self, target: Self::Id, resp: VoteCasted<Self::Id>) {
        self.send(target, Request::VoteCasted(resp));
    }

    fn entries_replicated(&self, target: Self::Id, resp: EntriesReplicated<Self::Id>) {
        self.send(target, Request::EntriesReplicated(resp));
    }

    fn replicate_entries(&self, target: Self::Id, req: AppendEntries<Self::Id>) {
        self.send(target, Request::AppendEntries(req));
    }
}

pub trait PersistentStorage {
    fn append_entries(&mut self, entries: Vec<Entry>);
    fn read_entries(&self, index: u64, max_count: usize) -> impl IterateEntries;
    fn remove_entries(&mut self, from: &EntryId);
    fn last_entry(&self) -> Option<EntryId>;
    fn previous_entry(&self, index: u64) -> Option<EntryId>;
    fn contains_entry(&self, entry_id: &EntryId) -> bool;

    fn append_entry(&mut self, term: u64, payload: Bytes) -> u64 {
        let index = self.next_index();

        self.append_entries(vec![Entry {
            index,
            term,
            payload,
        }]);

        index
    }

    fn last_entry_or_default(&self) -> EntryId {
        if let Some(entry) = self.last_entry() {
            return entry;
        }

        EntryId { index: 0, term: 0 }
    }

    /// We purposely require a mutable reference to enforce that only one location can mutate the
    /// persistent log entries.
    fn next_index(&mut self) -> u64 {
        if let Some(entry) = self.last_entry() {
            return entry.index + 1;
        }

        0
    }

    fn previous_entry_or_default(&self, index: u64) -> EntryId {
        if let Some(entry) = self.previous_entry(index) {
            return entry;
        }

        EntryId::new(0, 0)
    }
}

pub trait IterateEntries {
    fn next(&mut self) -> io::Result<Option<Entry>>;

    fn collect(mut self) -> io::Result<Vec<Entry>>
    where
        Self: Sized,
    {
        let mut entries = Vec::new();

        while let Some(entry) = self.next()? {
            entries.push(entry);
        }

        Ok(entries)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum State {
    Candidate,
    Follower,
    Leader,
}

pub struct TimeRange {
    low: u64,
    high: u64,
}

impl TimeRange {
    pub fn new(low: u64, high: u64) -> Self {
        Self { low, high }
    }

    pub fn new_timeout(&self) -> Duration {
        let mut rng = thread_rng();

        Duration::from_millis(rng.gen_range(self.low..self.high))
    }
}

pub struct Replica<Id> {
    id: Id,
    next_index: u64,
    match_index: u64,
    // When sending entries to replica represents the last index of the batch. If the replication
    // was successful, that value will be used to update the next_index value.
    batch_end_index: u64,
}

impl<Id> Replica<Id> {
    pub fn new(id: Id) -> Self {
        Self {
            id,
            next_index: 0,
            match_index: 0,
            batch_end_index: 0,
        }
    }
}

pub fn run_raft_app<NodeId, Storage, Command, R, S, D>(
    node_id: NodeId,
    seeds: Vec<NodeId>,
    time_range: TimeRange,
    mut storage: Storage,
    mut mailbox: R,
    sender: S,
    dispatcher: D,
) where
    NodeId: Ord + Hash + Clone,
    Storage: PersistentStorage,
    Command: UserCommand,
    S: RaftSender<Id = NodeId>,
    R: RaftRecv<Id = NodeId, Command = Command>,
    D: CommandDispatch<Command = Command>,
{
    let mut replicas = HashMap::new();
    let commit_index = 0u64;
    let election_timeout = time_range.new_timeout();
    let buffer = BytesMut::new();

    for seed_id in &seeds {
        let state = Replica::new(seed_id.clone());
        replicas.insert(seed_id.clone(), state);
    }

    let state = if seeds.is_empty() {
        State::Leader
    } else {
        State::Follower
    };

    let term = if let Some(entry_id) = storage.last_entry() {
        entry_id.term
    } else {
        0
    };

    let mut sm = RaftSM {
        id: node_id,
        term,
        state,
        commit_index,
        voted_for: None,
        tally: HashSet::new(),
        time: Instant::now(),
        election_timeout,
        inflights: VecDeque::new(),
        buffer,
        replicas,
    };

    while let Some(msg) = mailbox.recv() {
        match msg {
            Msg::RequestVote(args) => {
                sm.handle_request_vote(&sender, &storage, args);
            }

            Msg::AppendEntries(args) => {
                sm.handle_append_entries(&sender, &mut storage, Instant::now(), args);
            }

            Msg::VoteReceived(args) => {
                sm.handle_vote_received(&time_range, &storage, &sender, Instant::now(), args)
            }

            Msg::EntriesAppended(args) => {
                sm.handle_entries_appended(&dispatcher, args);
            }

            Msg::Command(cmd) => {
                sm.handle_command(&mut storage, &dispatcher, cmd);
            }

            Msg::Tick => {
                sm.handle_tick(&time_range, &storage, &sender, Instant::now());
            }

            Msg::Shutdown => {
                break;
            }
        }
    }
}
