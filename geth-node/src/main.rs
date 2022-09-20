use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use rand::{rngs::SmallRng, seq::IteratorRandom, SeedableRng};
use tokio::{
    select,
    sync::{mpsc, oneshot, watch},
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
    target: NodeId,
    payload: Payload,
}

enum Payload {
    Req(Req),
    Resp(Resp),
}

pub enum Req {
    AppendEntries(AppendEntries),
    RequestVote(RequestVote),
}

pub enum Resp {
    AppendEntries(AppendEntriesResponse),
    RequestVote(RequestVoteResponse),
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
    pub last_log_index: u64,
    pub last_log_term: u64,
}

pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

pub struct Entry {
    pub index: u64,
    pub data: u64,
}

pub struct Network {
    origin: NodeId,
    cluster: watch::Receiver<Vec<NodeId>>,
    mailbox: mpsc::UnboundedSender<Msg>,
}

impl Network {
    pub fn send_req(&self, target: NodeId, req: Req) {
        let _ = self.mailbox.send(Msg {
            origin: self.origin,
            target,
            payload: Payload::Req(req),
        });
    }

    pub fn send_resp(&self, target: NodeId, resp: Resp) {
        let _ = self.mailbox.send(Msg {
            origin: self.origin,
            target,
            payload: Payload::Resp(resp),
        });
    }
    /*    pub async fn request_vote(&self, target: NodeId, args: RequestVote) -> crate::Result<RequestVoteResponse> {
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
    */
    pub fn origin(&self) -> NodeId {
        self.origin
    }

    pub fn other_nodes(&self) -> Vec<NodeId> {
        self.cluster
            .borrow()
            .iter()
            .copied()
            .filter(|id| id != &self.origin)
            .collect()
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

fn generate_timeout(rng: &mut SmallRng, low: u64, high: u64) -> Duration {
    Duration::from_millis((low..high).choose(rng).unwrap())
}

async fn raft_node(
    network: Network,
    mut state: PersistentState,
    mut mailbox: mpsc::UnboundedReceiver<Msg>,
) {
    let mut rng = SmallRng::from_entropy();
    let mut commit_index = 0u64;
    let mut last_applied = 0u64;
    let mut status = Status::Follower;
    let mut timeout_election = generate_timeout(&mut rng, 150, 300);
    let mut clock = tokio::time::interval(Duration::from_millis(30));
    let mut last_recv_msg = Instant::now();

    loop {
        select! {
            Some(msg) = mailbox.recv() => {
                match msg.payload {
                    Payload::Req(req) => {
                        match req {
                            Req::AppendEntries(args) => {
                                if args.term >= state.current_term && status == Status::Candidate {
                                    status = Status::Follower;
                                }

                                if status == Status::Candidate {
                                    // We ignore the request because it comes from a node that got
                                    // recently demoted.
                                    continue;
                                }
                            }
                            Req::RequestVote(args) => {
                                let last_log_index = state.log.last().map(|e| e.index).unwrap_or(0);

                                let vote_granted = if args.term < state.current_term {
                                    false
                                } else if (state.voted_for.is_none() || state.voted_for == Some(args.candidate_id)) && args.last_log_index >= last_log_index {
                                    true
                                } else {
                                    false
                                };

                                network.send_resp(msg.origin, Resp::RequestVote(RequestVoteResponse {
                                    term: state.current_term,
                                    vote_granted,
                                }));
                            }
                        }
                    }

                    Payload::Resp(_) => {}
                }
            }

            _ = clock.tick() => {
                if last_recv_msg.elapsed() >= timeout_election {
                    state.voted_for = Some(network.origin());
                    state.current_term += 1;
                    status = Status::Candidate;

                    for node_id in network.other_nodes() {
                        network.send_req(node_id, Req::RequestVote(RequestVote{
                            term: state.current_term,
                            candidate_id: network.origin(),
                            last_log_index: todo!(),
                            last_log_term: todo!(),
                        }));
                    }
                }
            }
        }
    }
}

type Nodes = HashMap<NodeId, mpsc::UnboundedSender<Msg>>;

fn in_memory_network(node_count: usize) {
    let node_ids = std::iter::repeat(())
        .take(node_count)
        .map(|_| NodeId::new())
        .collect::<Vec<_>>();
    let mut nodes = HashMap::with_capacity(node_count);
    let (net_send, socket) = mpsc::unbounded_channel();
    let (cluster_changes, cluster_recv) = watch::channel(node_ids.clone());

    for node_id in node_ids {
        let (sender, mailbox) = mpsc::unbounded_channel();

        let network = Network {
            origin: node_id,
            cluster: cluster_recv.clone(),
            mailbox: net_send.clone(),
        };

        tokio::spawn(raft_node(network, PersistentState::new(), mailbox));
        nodes.insert(node_id, sender);
    }

    let config = NetworkConfig {
        nodes,
        cluster_changes,
    };

    tokio::spawn(in_memory_loop(config, socket));
}

struct NetworkConfig {
    nodes: Nodes,
    cluster_changes: watch::Sender<Vec<NodeId>>,
}

async fn in_memory_loop(config: NetworkConfig, mut socket: mpsc::UnboundedReceiver<Msg>) {
    while let Some(msg) = socket.recv().await {
        if let Some(mailbox) = config.nodes.get(&msg.target) {
            let _ = mailbox.send(msg);
        }
    }
}
