use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::time::{Duration, Instant};

use bytes::BytesMut;

use crate::entry::EntryId;
use crate::msg::{
    AppendEntries, EntriesAppended, EntriesReplicated, RequestVote, VoteCasted, VoteReceived,
};
use crate::{
    CommandDispatch, IterateEntries, PersistentStorage, RaftSender, Replica, State, TimeRange,
    UserCommand,
};

pub struct RaftSM<NodeId, Command> {
    pub id: NodeId,
    pub term: u64,
    pub state: State,
    pub commit_index: u64,
    pub voted_for: Option<NodeId>,
    pub tally: HashSet<NodeId>,
    pub time: Instant,
    pub election_timeout: Duration,
    pub inflights: VecDeque<(u64, Command)>,
    pub buffer: BytesMut,
    pub replicas: HashMap<NodeId, Replica<NodeId>>,
}

impl<NodeId, Command> RaftSM<NodeId, Command>
where
    NodeId: Clone + Ord + Hash,
    Command: UserCommand,
{
    pub fn handle_request_vote<S, P>(&mut self, sender: &S, storage: &P, args: RequestVote<NodeId>)
    where
        S: RaftSender<Id = NodeId>,
        P: PersistentStorage,
    {
        if args.term < self.term {
            sender.vote_casted(
                args.candidate_id,
                VoteCasted {
                    node_id: self.id.clone(),
                    term: self.term,
                    granted: false,
                },
            );

            return;
        }

        let granted: bool;
        if self.term < args.term || self.voted_for.is_none() {
            self.term = args.term;

            if let Some(last_entry_id) = storage.last_entry() {
                granted = last_entry_id.index <= args.last_log_index
                    && last_entry_id.term <= args.last_log_term;

                if granted {
                    self.voted_for = Some(args.candidate_id.clone());
                    self.state = State::Follower;
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

            granted = self.voted_for == Some(args.candidate_id.clone())
                && last_entry_id.index <= args.last_log_index
                && last_entry_id.term <= args.last_log_term;
        }

        sender.vote_casted(
            args.candidate_id,
            VoteCasted {
                node_id: self.id.clone(),
                term: self.term,
                granted,
            },
        )
    }

    pub fn handle_append_entries<S, P>(
        &mut self,
        sender: &S,
        storage: &mut P,
        now: Instant,
        args: AppendEntries<NodeId>,
    ) where
        S: RaftSender<Id = NodeId>,
        P: PersistentStorage,
    {
        if self.term > args.term {
            sender.entries_replicated(
                args.leader_id,
                EntriesReplicated {
                    node_id: self.id.clone(),
                    term: self.term,
                    success: false,
                },
            );

            return;
        }

        if self.term < args.term {
            self.voted_for = None;
            self.term = args.term;
        }

        self.time = now;
        self.state = State::Follower;

        // Checks if we have a point of reference with the leader.
        if !storage.contains_entry(&EntryId::new(args.prev_log_index, args.prev_log_term)) {
            sender.entries_replicated(
                args.leader_id,
                EntriesReplicated {
                    node_id: self.id.clone(),
                    term: self.term,
                    success: false,
                },
            );

            return;
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
                    node_id: self.id.clone(),
                    term: self.term,
                    success: true,
                },
            );

            return;
        }

        // We truncate on the spot entries that were not committed by the previous leader.
        if let Some(last) = storage.last_entry() {
            if last.index > args.prev_log_index && last.term != args.term {
                storage.remove_entries(&EntryId::new(args.prev_log_index, args.prev_log_term));
            }
        }

        storage.append_entries(args.entries);

        if args.leader_commit > self.commit_index {
            self.commit_index = min(args.leader_commit, last_entry_index);
        }

        sender.entries_replicated(
            args.leader_id,
            EntriesReplicated {
                node_id: self.id.clone(),
                term: self.term,
                success: true,
            },
        );
    }

    pub fn handle_vote_received<P, S>(
        &mut self,
        time_range: &TimeRange,
        storage: &P,
        sender: &S,
        now: Instant,
        args: VoteReceived<NodeId>,
    ) where
        P: PersistentStorage,
        S: RaftSender<Id = NodeId>,
    {
        // Probably out-of-order message.
        if self.term > args.term || self.state == State::Leader {
            return;
        }

        if self.term < args.term {
            self.term = args.term;
            self.state = State::Follower;
            self.time = now;
            self.election_timeout = time_range.new_timeout();

            return;
        }

        if args.granted {
            self.tally.insert(args.node_id);

            // If the cluster reached quorum
            if self.tally.len() + 1 >= (self.replicas.len() + 1) / 2 {
                self.state = State::Leader;

                let last_index = storage.last_entry().map(|e| e.index).unwrap_or_default();
                for replica in self.replicas.values_mut() {
                    replica.next_index = last_index + 1;
                    replica.match_index = 0;
                }

                self.replicate_entries(storage, sender);
            }
        }
    }

    pub fn handle_entries_appended<D>(&mut self, dispatcher: &D, args: EntriesAppended<NodeId>)
    where
        D: CommandDispatch<Command = Command>,
    {
        if self.state != State::Leader {
            return;
        }

        if let Some(replica) = self.replicas.get_mut(&args.node_id) {
            if args.success {
                replica.match_index = replica.batch_end_index;
                replica.next_index = replica.batch_end_index + 1;

                let mut lowest_replicated_index = u64::MAX;
                for replica in self.replicas.values() {
                    lowest_replicated_index = min(lowest_replicated_index, replica.match_index);
                }

                // Report all commands that got successfully replicated.
                while let Some((index, cmd)) = self.inflights.pop_front() {
                    if index <= lowest_replicated_index {
                        dispatcher.dispatch(cmd);
                    } else {
                        // We reached a point where we didn't receive acknowledgement that
                        // pass that point, the commands got replicated.
                        self.inflights.push_front((index, cmd));
                        break;
                    }
                }

                self.commit_index = lowest_replicated_index;
            } else {
                // FIXME - This is the simplest way of handling this. On large dataset, it
                // could be beneficial for the replica to actually send an hint of where
                // its log actually is.
                replica.next_index = replica.next_index.saturating_sub(1);
            }
        }
    }

    pub fn handle_command<D, P>(&mut self, storage: &mut P, dispatcher: &D, cmd: Command)
    where
        P: PersistentStorage,
        D: CommandDispatch<Command = Command>,
    {
        // If we are dealing with a write command but are not the leader of the cluster,
        // we must refuse to serve the command.
        //
        // NOTE - Depending on the use case, it might not be ok to serve read command if
        // we are not the leader either. It the node is lagging behind replication-wise,
        // the user might get different view of the data whether they are reading from the
        // leader node or not.
        if !cmd.is_read() && self.state != State::Leader {
            cmd.reject();
            return;
        }

        // We persist the command on our side. If we replicated in enough node, we will
        // let the command through.
        cmd.write(&mut self.buffer);
        let index = storage.append_entry(self.term, self.buffer.split().freeze());

        // We are in single-node, we can dispatch the command to the upper layers of node.
        if self.replicas.is_empty() {
            dispatcher.dispatch(cmd);
        } else {
            // TODO - we keep recent commands in-memory until replication is successful.
            // NOTE: It could cause rapid memory increase if the node is under heavy load.
            // Another approach would be to read the command back from the persistent
            // storage. Doing so cause unnecessary back & forth (de)serialization.
            self.inflights.push_back((index, cmd));
        }
    }

    pub fn handle_tick<P, S>(
        &mut self,
        time_range: &TimeRange,
        storage: &P,
        sender: &S,
        now: Instant,
    ) where
        P: PersistentStorage,
        S: RaftSender<Id = NodeId>,
    {
        // In single-node we don't need to communicate with other nodes.
        if self.replicas.is_empty() {
            return;
        }

        if self.state == State::Leader {
            self.replicate_entries(storage, sender);
        } else if self.time.duration_since(now) >= self.election_timeout {
            // We didn't hear form the leader a long time ago, time to start a new election.
            self.state = State::Candidate;
            self.term += 1;
            self.voted_for = Some(self.id.clone());
            self.election_timeout = time_range.new_timeout();
            self.time = now;

            let last_entry = storage.last_entry_or_default();
            for replica in self.replicas.values() {
                sender.request_vote(
                    replica.id.clone(),
                    RequestVote {
                        term: self.term,
                        candidate_id: self.id.clone(),
                        last_log_index: last_entry.index,
                        last_log_term: last_entry.term,
                    },
                );
            }
        }
    }

    pub fn replicate_entries<P, S>(&self, storage: &P, sender: &S)
    where
        P: PersistentStorage,
        S: RaftSender<Id = NodeId>,
    {
        for replica in self.replicas.values() {
            let prev_entry = storage.previous_entry_or_default(replica.next_index);

            let entries = storage.read_entries(prev_entry.index, 500);

            match entries.collect() {
                Err(_) => {
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
                            term: self.term,
                            leader_id: self.id.clone(),
                            prev_log_index: prev_entry.index,
                            prev_log_term: prev_entry.term,
                            leader_commit: self.commit_index,
                            entries,
                        },
                    );
                }
            }
        }
    }
}
