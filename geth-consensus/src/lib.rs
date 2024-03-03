use std::cmp::min;
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

mod entry;
mod msg;

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

fn run_raft_app<NodeId, Storage, Command, R, S, D>(
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
    let mut tally = HashSet::<NodeId>::new();
    let mut replicas = HashMap::new();
    let mut commit_index = 0u64;
    let mut last_applied = 0u64;
    let mut voted_for = None;
    let mut time_tracker = Instant::now();
    let mut election_timeout = time_range.new_timeout();
    let mut buffer = BytesMut::new();
    let mut inflights = VecDeque::new();

    for seed_id in &seeds {
        let state = Replica::new(seed_id.clone());
        replicas.insert(seed_id.clone(), state);
    }

    let mut state = if seeds.is_empty() {
        State::Leader
    } else {
        State::Follower
    };

    let mut term = if let Some(entry_id) = storage.last_entry() {
        entry_id.term
    } else {
        0
    };

    while let Some(msg) = mailbox.recv() {
        match msg {
            Msg::RequestVote(args) => {
                if args.term < term {
                    sender.vote_casted(
                        args.candidate_id,
                        VoteCasted {
                            node_id: node_id.clone(),
                            term,
                            granted: false,
                        },
                    );

                    continue;
                }

                let mut granted = false;
                if term < args.term || voted_for.is_none() {
                    term = args.term;

                    if let Some(last_entry_id) = storage.last_entry() {
                        granted = last_entry_id.index <= args.last_log_index
                            && last_entry_id.term <= args.last_log_term;

                        if granted {
                            voted_for = Some(args.candidate_id.clone());
                            state = State::Follower;
                        }
                    } else {
                        granted = true;
                    }
                } else {
                    let last_entry_id = if let Some(last) = storage.last_entry() {
                        last
                    } else {
                        EntryId::default()
                    };

                    granted = voted_for == Some(args.candidate_id.clone())
                        && last_entry_id.index <= args.last_log_index
                        && last_entry_id.term <= args.last_log_term;
                }

                sender.vote_casted(
                    args.candidate_id,
                    VoteCasted {
                        node_id: node_id.clone(),
                        term,
                        granted,
                    },
                )
            }

            Msg::AppendEntries(args) => {
                if term > args.term {
                    sender.entries_replicated(
                        args.leader_id,
                        EntriesReplicated {
                            node_id: node_id.clone(),
                            term,
                            success: false,
                        },
                    );

                    continue;
                }

                if term < args.term {
                    voted_for = None;
                    term = args.term;
                }

                time_tracker = Instant::now();
                state = State::Follower;

                // Checks if we have a point of reference with the leader.
                if !storage.contains_entry(&EntryId::new(args.prev_log_index, args.prev_log_term)) {
                    sender.entries_replicated(
                        args.leader_id,
                        EntriesReplicated {
                            node_id: node_id.clone(),
                            term,
                            success: false,
                        },
                    );

                    continue;
                }

                let last_entry_index = if let Some(last) = args.entries.last() {
                    last.index
                } else {
                    u64::MAX
                };

                // Means it was a heartbeat from the leader node.
                if args.entries.is_empty() {
                    sender.entries_replicated(
                        args.leader_id,
                        EntriesReplicated {
                            node_id: node_id.clone(),
                            term,
                            success: true,
                        },
                    );

                    continue;
                }

                // We truncate on the spot entries that were not committed by the previous leader.
                if let Some(last) = storage.last_entry() {
                    if last.index > args.prev_log_index && last.term != args.term {
                        storage
                            .remove_entries(&EntryId::new(args.prev_log_index, args.prev_log_term));
                    }
                }

                storage.append_entries(args.entries);

                if args.leader_commit > commit_index {
                    commit_index = min(args.leader_commit, last_entry_index);
                }

                sender.entries_replicated(
                    args.leader_id,
                    EntriesReplicated {
                        node_id: node_id.clone(),
                        term,
                        success: true,
                    },
                );
            }

            Msg::VoteReceived(args) => {
                // Probably out-of-order message.
                if term > args.term || state == State::Leader {
                    continue;
                }

                if term < args.term {
                    term = args.term;
                    state = State::Follower;
                    time_tracker = Instant::now();
                    election_timeout = time_range.new_timeout();

                    continue;
                }

                if args.granted {
                    tally.insert(args.node_id);

                    // If the cluster reached quorum
                    if tally.len() + 1 >= (seeds.len() + 1) / 2 {
                        state = State::Leader;

                        let last_index = storage.last_entry().map(|e| e.index).unwrap_or_default();
                        for replica in replicas.values_mut() {
                            replica.next_index = last_index + 1;
                            replica.match_index = 0;
                        }
                    }
                }
            }

            Msg::EntriesAppended(args) => {
                if state != State::Leader {
                    continue;
                }

                if let Some(replica) = replicas.get_mut(&args.node_id) {
                    if args.success {
                        replica.match_index = replica.batch_end_index;
                        replica.next_index = replica.batch_end_index + 1;

                        let mut lowest_replicated_index = u64::MAX;
                        for replica in replicas.values() {
                            lowest_replicated_index =
                                min(lowest_replicated_index, replica.match_index);
                        }

                        // Report all commands that got successfully replicated.
                        while let Some((index, cmd)) = inflights.pop_front() {
                            if index <= lowest_replicated_index {
                                dispatcher.dispatch(cmd);
                            } else {
                                // We reached a point where we didn't receive acknowledgement that
                                // pass that point, the commands got replicated.
                                inflights.push_front((index, cmd));
                                break;
                            }
                        }

                        commit_index = lowest_replicated_index;
                    } else {
                        // FIXME - This is the simplest way of handling this. On large dataset, it
                        // could be beneficial for the replica to actually send an hint of where
                        // its log actually is.
                        replica.next_index = replica.next_index.saturating_sub(1);
                    }
                }
            }

            Msg::Command(cmd) => {
                // If we are dealing with a write command but are not the leader of the cluster,
                // we must refuse to serve the command.
                //
                // NOTE - Depending on the use case, it might not be ok to serve read command if
                // we are not the leader either. It the node is lagging behind replication-wise,
                // the user might get different view of the data whether they are reading from the
                // leader node or not.
                if !cmd.is_read() && state != State::Leader {
                    cmd.reject();
                    continue;
                }

                // We persist the command on our side. If we replicated in enough node, we will
                // let the command through.
                cmd.write(&mut buffer);
                let index = storage.append_entry(term, buffer.split().freeze());

                // We are in single-node, we can dispatch the command to the upper layers of node.
                if seeds.is_empty() {
                    dispatcher.dispatch(cmd);
                } else {
                    // TODO - we keep recent commands in-memory until replication is successful.
                    // NOTE: It could cause rapid memory increase if the node is under heavy load.
                    // Another approach would be to read the command back from the persistent
                    // storage. Doing so cause unnecessary back & forth (de)serialization.
                    inflights.push_back((index, cmd));
                }
            }

            Msg::Tick => {
                // In single-node we don't need to communicate with other nodes.
                if seeds.is_empty() {
                    return;
                }

                if state == State::Leader {
                    for replica in replicas.values() {
                        let prev_entry = storage.previous_entry_or_default(replica.next_index);

                        let entries = storage.read_entries(prev_entry.index, 500);

                        match entries.collect() {
                            Err(e) => {
                                // Best course of action would be to crash the server. I don't see
                                // immediate ways to recover from this, especially as a leader.
                                // TODO - Write an ERROR log statement to trace what exactly went
                                // wrong.
                                break;
                            }

                            Ok(entries) => {
                                sender.replicate_entries(
                                    replica.id.clone(),
                                    AppendEntries {
                                        term,
                                        leader_id: node_id.clone(),
                                        prev_log_index: prev_entry.index,
                                        prev_log_term: prev_entry.term,
                                        leader_commit: commit_index,
                                        entries,
                                    },
                                );
                            }
                        }
                    }
                } else if time_tracker.elapsed() >= election_timeout {
                    // We didn't hear form the leader a long time ago, time to start a new election.
                    state = State::Candidate;
                    term += 1;
                    voted_for = Some(node_id.clone());
                    election_timeout = time_range.new_timeout();
                    time_tracker = Instant::now();

                    let last_entry = storage.last_entry_or_default();
                    for replica in replicas.values() {
                        sender.request_vote(
                            replica.id.clone(),
                            RequestVote {
                                term,
                                candidate_id: node_id.clone(),
                                last_log_index: last_entry.index,
                                last_log_term: last_entry.term,
                            },
                        );
                    }
                }
            }

            Msg::Shutdown => {
                break;
            }
        }
    }
}
