use crate::names::types::STREAM_DELETED;
use crate::process::messages::{Messages, SubscribeRequests, SubscribeResponses};
use crate::process::{Item, ProcessEnv};
use geth_common::Record;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;

const ALL_IDENT: &str = "$all";

#[derive(Default)]
struct Register {
    inner: HashMap<String, Vec<UnboundedSender<Messages>>>,
}

impl Register {
    fn register(&mut self, key: String, sender: UnboundedSender<Messages>) {
        self.inner.entry(key).or_default().push(sender);
    }

    fn publish(&mut self, record: Record) {
        if let Some(senders) = self.inner.get_mut(&record.stream_name) {
            senders.retain(|sender| {
                sender
                    .send(SubscribeResponses::Record(record.clone()).into())
                    .is_ok()
                    && record.class != STREAM_DELETED
            });
        }

        if let Some(senders) = self.inner.get_mut(ALL_IDENT) {
            senders.retain(|sender| {
                sender
                    .send(SubscribeResponses::Record(record.clone()).into())
                    .is_ok()
            });
        }
    }
}

pub async fn run(mut env: ProcessEnv) -> eyre::Result<()> {
    let mut reg = Register::default();
    while let Some(item) = env.queue.recv().await {
        match item {
            Item::Stream(stream) => {
                if let Ok(req) = stream.payload.try_into() {
                    match req {
                        SubscribeRequests::Subscribe { ident } => {
                            if stream
                                .sender
                                .send(SubscribeResponses::Confirmed.into())
                                .is_ok()
                            {
                                reg.register(ident, stream.sender);
                            }
                        }
                        _ => {
                            tracing::warn!(
                                "unsupported subscription streaming request {}",
                                stream.correlation
                            );
                        }
                    }

                    continue;
                }

                tracing::warn!(
                    "malformed reader request from stream request {}",
                    stream.correlation
                );
            }

            Item::Mail(mail) => {
                if let Ok(req) = mail.payload.try_into() {
                    match req {
                        SubscribeRequests::Push { events } => {
                            // We don't really to confirm to the entity that sent us the push request to deliver those events first.
                            env.client.reply(
                                mail.origin,
                                mail.correlation,
                                SubscribeResponses::Confirmed.into(),
                            )?;

                            for event in events {
                                reg.publish(event);
                            }
                        }

                        _ => {
                            tracing::warn!("unsupported subscription request {}", mail.correlation);
                        }
                    }
                    continue;
                }
            }
        }
    }

    Ok(())
}
