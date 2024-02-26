use std::cmp::min;
use std::io;
use std::time::Instant;

use bytes::BytesMut;

use crate::entry::{Entry, EntryId};
use crate::msg::{
    AppendEntries, EntriesAppended, EntriesReplicated, RequestVote, VoteCasted, VoteReceived,
};

mod entry;
mod msg;

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
}

pub trait RaftRecv {
    type Id: Ord;
    type Command: UserCommand;

    fn recv(&mut self) -> Option<Msg<Self::Id, Self::Command>>;
}

pub trait RaftSender {
    type Id: Ord;

    fn send(&self, target: Self::Id, request: Request<Self::Id>);

    fn vote_casted(&self, target: Self::Id, resp: VoteCasted<Self::Id>) {
        self.send(target, Request::VoteCasted(resp));
    }

    fn entries_replicated(&self, target: Self::Id, resp: EntriesReplicated<Self::Id>) {
        self.send(target, Request::EntriesReplicated(resp));
    }
}

pub trait PersistentStorage {
    fn append_entries(&mut self, entries: Vec<Entry>);
    fn read_entries(&self, index: u64, max_count: u64) -> impl IterateEntries;
    fn remove_entries(&mut self, from: &EntryId);
    fn last_entry(&self) -> Option<EntryId>;
    fn previous_entry(&self, index: u64) -> Option<EntryId>;
    fn contains_entry(&self, entry_id: &EntryId) -> bool;
}

pub trait IterateEntries {
    fn next(&mut self) -> io::Result<Option<Entry>>;
}

pub enum State {
    Candidate,
    Follower,
    Leader,
}

fn run_raft_app<NodeId, Storage, Command, R, S, D>(
    node_id: NodeId,
    seeds: Vec<NodeId>,
    mut storage: Storage,
    mut mailbox: R,
    sender: S,
) where
    NodeId: Ord + Clone,
    Storage: PersistentStorage,
    Command: UserCommand,
    S: RaftSender<Id = NodeId>,
    R: RaftRecv<Id = NodeId, Command = Command>,
    D: CommandDispatch<Command = Command>,
{
    let mut commit_index = 0u64;
    let mut voted_for = None;
    let mut election_timeout = Instant::now();
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

                election_timeout = Instant::now();
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

            Msg::VoteReceived(_) => {}
            Msg::EntriesAppended(_) => {}
            Msg::Command(_) => {}
            Msg::Tick => {}
            Msg::Shutdown => {
                break;
            }
        }
    }
}
