use crate::process::subscription::Request;
use crate::process::{Item, ProcessEnv, ProcessRawEnv, Runnable, RunnableRaw};
use bytes::{Buf, Bytes};
use geth_common::ReadCompleted;
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::storage::Storage;
use geth_mikoshi::wal::chunks::ChunkContainer;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;

const ALL_IDENT: Bytes = Bytes::from_static(b"$all");

#[derive(Default)]
struct Register {
    inner: HashMap<Bytes, Vec<UnboundedSender<Bytes>>>,
}

impl Register {
    fn register(&mut self, key: Bytes, sender: UnboundedSender<Bytes>) {
        self.inner.entry(key).or_default().push(sender);
    }

    fn publish(&mut self, ident: &Bytes, payload: Bytes) {
        if let Some(senders) = self.inner.get_mut(ident) {
            senders.retain(|sender| sender.send(payload.clone()).is_ok());
        }

        if let Some(senders) = self.inner.get_mut(&ALL_IDENT) {
            senders.retain(|sender| sender.send(payload.clone()).is_ok());
        }
    }
}

pub struct PubSub;

#[async_trait::async_trait]
impl Runnable for PubSub {
    fn name(&self) -> &'static str {
        "subscription"
    }

    async fn run(self: Box<Self>, mut env: ProcessEnv) -> eyre::Result<()> {
        let mut reg = Register::default();
        while let Some(item) = env.queue.recv().await {
            match item {
                Item::Stream(stream) => {
                    if let Some(req) = Request::try_from(stream.payload) {
                        match req {
                            Request::Subscribe { ident } => {
                                reg.register(ident, stream.sender);
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
                    if let Some(req) = Request::try_from(mail.payload) {
                        match req {
                            Request::Push { mut events } => {
                                while events.has_remaining() {
                                    let mut temp = events.clone();
                                    let size = temp.get_u32_le() as usize;
                                    temp.advance(size_of::<u64>()); // position;

                                    // There is no need to deal with events that doesn't hold
                                    // data.
                                    if temp.get_u8() != 0 {
                                        events.advance(size_of::<u32>() + size + size_of::<u32>());
                                        continue;
                                    }

                                    let str_len = events.get_u16_le() as usize;
                                    let ident = events.copy_to_bytes(str_len);

                                    events.advance(size_of::<u32>());
                                    let event = events.copy_to_bytes(size);

                                    debug_assert_eq!(
                                        size,
                                        events.get_u32_le() as usize,
                                        "pre and after sizes don't match!"
                                    );

                                    reg.publish(&ident, event);
                                }
                            }

                            Request::Unsubscribe { ident } => {}

                            _ => {
                                tracing::warn!(
                                    "unsupported subscription request {}",
                                    mail.correlation
                                );
                            }
                        }
                        continue;
                    }
                }
            }
        }

        Ok(())
    }
}
