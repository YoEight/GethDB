use crate::bus::SubscribeMsg;
use crate::messages::{SubscriptionConfirmed, SubscriptionTarget};
use geth_mikoshi::{Entry, MikoshiStream};
use std::collections::HashMap;
use tokio::sync::mpsc;
use uuid::Uuid;

enum Msg {
    Subscribe(SubscribeMsg),
    EventCommitted(Entry),
}

#[derive(Clone)]
pub struct SubscriptionsClient {
    inner: mpsc::UnboundedSender<Msg>,
}

impl SubscriptionsClient {
    pub fn subscribe(&self, msg: SubscribeMsg) -> eyre::Result<()> {
        if self.inner.send(Msg::Subscribe(msg)).is_err() {
            eyre::bail!("Main bus has shutdown!");
        }

        Ok(())
    }

    pub fn event_committed(&self, event: Entry) -> eyre::Result<()> {
        if self.inner.send(Msg::EventCommitted(event)).is_err() {
            eyre::bail!("Main bus has shutdown!");
        }

        Ok(())
    }
}

pub fn start() -> SubscriptionsClient {
    let (sender, mailbox) = mpsc::unbounded_channel();

    tokio::spawn(service(mailbox));

    SubscriptionsClient { inner: sender }
}

struct Sub {
    stream: String,
    sender: mpsc::Sender<Entry>,
}

async fn service(mut mailbox: mpsc::UnboundedReceiver<Msg>) {
    let mut registry = HashMap::<String, Vec<Sub>>::new();
    while let Some(msg) = mailbox.recv().await {
        match msg {
            Msg::Subscribe(msg) => match msg.payload.target {
                SubscriptionTarget::Stream(opts) => {
                    let subs = registry.entry(opts.stream_name.clone()).or_default();
                    let (sender, reader) = mpsc::channel(500);

                    subs.push(Sub {
                        stream: opts.stream_name,
                        sender,
                    });

                    let _ = msg.mail.send(SubscriptionConfirmed {
                        correlation: Uuid::new_v4(),
                        reader: MikoshiStream::new(reader),
                    });
                }

                SubscriptionTarget::Process(_) => {
                    // TODO
                }
            },

            Msg::EventCommitted(entry) => {
                if let Some(subs) = registry.get(&entry.stream_name) {
                    for sub in subs {
                        if sub.stream != entry.stream_name {
                            continue;
                        }

                        if let Err(_) = sub.sender.send(entry.clone()).await {
                            // TODO - implement unsubscription logic here.
                        }
                    }
                }
            }
        }
    }

    tracing::info!("storage service exited");
}
