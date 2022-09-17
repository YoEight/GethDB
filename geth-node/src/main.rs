use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use uuid::Uuid;

fn main() {
    println!("Hello, world!");
}

pub type Result<A> = std::result::Result<A, Error>;

pub enum Error {
    Timeout,
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct NodeId(Uuid);

impl NodeId {
    pub fn new() -> Self {
        NodeId(uuid::Uuid::new_v4())
    }
}

struct Msg {
    origin: NodeId,
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
    origin: NodeId,
    inner: mpsc::UnboundedSender<Msg>,
}

impl Network {
    pub async fn request_vote(&self, args: RequestVote) -> crate::Result<RequestVoteResponse> {
        let (resp, result) = oneshot::channel();

        let _ = self.inner.send(Msg {
            origin: self.origin,
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
            origin: self.origin,
            rpc: Rpc::AppendEntries { args, resp },
        });

        result.await.expect("fatal error")
    }
}

struct PersistentState {
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<Entry>,
}

impl PersistentState {
    fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum Status {
    Follower,
    Leader,
    Candidate,
}

async fn raft_node(
    network: Network,
    mut state: PersistentState,
    mut mailbox: mpsc::UnboundedReceiver<Msg>,
) {
    let mut commit_index = 0u64;
    let mut last_applied = 0u64;
    let mut status = Status::Follower;
    let mut timeout_election = Duration::from_millis(150);
    let mut clock = tokio::time::interval(Duration::from_millis(30));
    let mut last_recv_msg = Instant::now();

    loop {
        select! {
            Some(msg) = mailbox.recv() => {
                match msg.rpc {
                    Rpc::AppendEntries { args, resp } => {}
                    Rpc::RequestVote { args, resp } => {
                        if args.term < state.current_term {
                            let _ = resp.send(Ok(RequestVoteResponse {
                                term: state.current_term,
                                vote_granted: false,
                            }));
                        }
                    }
                }
            }

            _ = clock.tick() => {
                if last_recv_msg.elapsed() >= timeout_election {
                    // TODO - Vote for myself.
                    state.current_term += 1;
                    status = Status::Candidate;
                    network.request_vote(RequestVote {
                        term: state.current_term,
                        candidate_id: todo!(),
                        last_log_index: todo!(),
                        last_log_term: todo!(),
                    }).await;
                }
            }
        }
    }
}

type Nodes = HashMap<NodeId, mpsc::UnboundedSender<Msg>>;

fn in_memory_network(node_count: usize) {
    let mut nodes = HashMap::with_capacity(node_count);
    let (net_send, socket) = mpsc::unbounded_channel();

    for _ in std::iter::repeat(()).take(node_count) {
        let node_id = NodeId::new();
        let (sender, mailbox) = mpsc::unbounded_channel();

        let network = Network {
            origin: node_id,
            inner: net_send.clone(),
        };

        tokio::spawn(raft_node(network, PersistentState::new(), mailbox));

        nodes.insert(node_id, sender);
    }

    tokio::spawn(in_memory_loop(nodes, socket));
}

async fn in_memory_loop(nodes: Nodes, mut socket: mpsc::UnboundedReceiver<Msg>) {
    while let Some(msg) = socket.recv().await {
        match &msg.rpc {
            Rpc::AppendEntries { args, resp } => {}
            Rpc::RequestVote { args, resp } => {}
        }
    }
}
