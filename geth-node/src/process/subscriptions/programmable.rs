use crate::process::subscriptions::SubscriptionsClient;
use geth_mikoshi::Entry;
use pyro_runtime::Engine;
use tokio::sync::mpsc;

enum Msg {
    EventAppeared(Entry),
}

pub struct Programmable {
    streams: Vec<String>,
    sender: mpsc::UnboundedSender<Msg>,
}

pub fn spawn(client: SubscriptionsClient, output: mpsc::Sender<Entry>, source_code: &str) {
    let engine = Engine::builder();
}
