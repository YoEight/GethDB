use crate::entry::Entry;

#[derive(Debug)]
pub struct RequestVote<Id> {
    pub term: u64,
    pub candidate_id: Id,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug)]
pub struct AppendEntries<Id> {
    pub term: u64,
    pub leader_id: Id,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub leader_commit: u64,
    pub entries: Vec<Entry>,
}

#[derive(Debug)]
pub struct EntriesReplicated<Id> {
    pub node_id: Id,
    pub term: u64,
    pub success: bool,
}

#[derive(Debug)]
pub struct VoteCasted<Id> {
    pub node_id: Id,
    pub term: u64,
    pub granted: bool,
}

#[derive(Debug)]
pub struct VoteReceived<Id> {
    pub node_id: Id,
    pub term: u64,
    pub granted: bool,
}

#[derive(Debug)]
pub struct EntriesAppended<Id> {
    pub node_id: Id,
    pub term: u64,
    pub success: bool,
}
