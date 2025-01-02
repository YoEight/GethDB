use crate::process::messages::{Messages, SubscribeRequests, SubscribeResponses};
use crate::process::subscription::Request;
use crate::process::{Item, ProcessEnv};
use bytes::Buf;
use geth_mikoshi::wal::LogEntry;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;

const ALL_IDENT: &'static str = "$all";

#[derive(Default)]
struct Register {
    inner: HashMap<String, Vec<UnboundedSender<Messages>>>,
}

impl Register {
    fn register(&mut self, key: String, sender: UnboundedSender<Messages>) {
        self.inner.entry(key).or_default().push(sender);
    }

    fn publish(&mut self, ident: &String, payload: LogEntry) {
        if let Some(senders) = self.inner.get_mut(ident) {
            senders.retain(|sender| {
                sender
                    .send(SubscribeResponses::Entry(payload.clone()).into())
                    .is_ok()
            });
        }

        if let Some(senders) = self.inner.get_mut(ALL_IDENT) {
            senders.retain(|sender| {
                sender
                    .send(SubscribeResponses::Entry(payload.clone()).into())
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
                if let Some(req) = stream.payload.try_into().ok() {
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
                if let Some(req) = mail.payload.try_into().ok() {
                    match req {
                        SubscribeRequests::Push { events } => {
                            for event in events {
                                if event.r#type != 0 {
                                    continue;
                                }

                                let mut sub_entry = event.payload.clone();

                                let str_len = sub_entry.get_u16_le() as usize;
                                let ident = unsafe {
                                    String::from_utf8_unchecked(
                                        sub_entry.copy_to_bytes(str_len).to_vec(),
                                    )
                                };

                                reg.publish(&ident, event);
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
