use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

fn main() {
    println!("Hello, world!");
}

pub type Result<A> = std::result::Result<A, Error>;

pub enum Error {
    Timeout,
}

#[derive(Debug, Copy, Clone)]
pub struct NodeId(Uuid);

struct Msg {
    target: NodeId,
    rpc: Rpc,
}

enum Rpc {
    AppendEntries {
        args: AppendEntries,
        resp: oneshot::Sender<crate::Result<AppendEntriesResponse>>,
    },

    RequestVote {
        args: RequestVote,
        resp: oneshot::Sender<crate::Result<RequestVoteResponse>>,
    },
}

pub struct AppendEntries {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: usize,
    pub prev_log_term: u64,
    pub entries: Vec<Entry>,
    pub leader_commit: usize,
}

pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}

pub struct RequestVote {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: usize,
    pub last_log_term: usize,
}

pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

pub struct Entry {
    pub index: usize,
    pub data: usize,
}

pub struct Network {
    inner: mpsc::UnboundedSender<Msg>,
}

impl Network {
    pub async fn request_vote(
        &self,
        target: NodeId,
        args: RequestVote,
    ) -> crate::Result<RequestVoteResponse> {
        let (resp, result) = oneshot::channel();

        let _ = self.inner.send(Msg {
            target,
            rpc: Rpc::RequestVote { args, resp },
        });

        result.await.expect("fatal error")
    }

    pub async fn append_entries(
        &self,
        target: NodeId,
        args: AppendEntries,
    ) -> crate::Result<AppendEntriesResponse> {
        let (resp, result) = oneshot::channel();

        let _ = self.inner.send(Msg {
            target,
            rpc: Rpc::AppendEntries { args, resp },
        });

        result.await.expect("fatal error")
    }
}
