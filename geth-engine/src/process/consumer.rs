use std::{collections::VecDeque, usize};

use geth_common::{
    Direction, ReadStreamCompleted, Record, Revision, SubscriptionConfirmation, SubscriptionEvent,
    UnsubscribeReason,
};
use geth_mikoshi::hashing::mikoshi_hash;
use tokio::select;
use tracing::instrument;

use crate::{
    process::subscription::{self, SubscriptionClient},
    reading, IndexClient, ReaderClient, RequestContext,
};

struct CatchingUp {
    reader: reading::Streaming,
    sub: subscription::Streaming,
    end: u64,
    history: VecDeque<Record>,
    conf: Option<SubscriptionConfirmation>,
}

struct PlayHistory {
    history: VecDeque<Record>,
    sub: subscription::Streaming,
}

enum State {
    Init,
    CatchingUp(CatchingUp),
    PlayHistory(PlayHistory),
    Live(subscription::Streaming),
}

pub struct ConsumerClient {
    context: RequestContext,
    state: State,
    done: bool,
    stream_name: String,
    start: Revision<u64>,
    reader: ReaderClient,
    sub: SubscriptionClient,
    index: IndexClient,
}

impl ConsumerClient {
    pub fn new(
        context: RequestContext,
        stream_name: String,
        start: Revision<u64>,
        reader: ReaderClient,
        sub: SubscriptionClient,
        index: IndexClient,
    ) -> Self {
        Self {
            state: State::Init,
            done: false,
            context,
            stream_name,
            start,
            reader,
            sub,
            index,
        }
    }

    // CAUTION: a situation where an user is reading very far away from the head of the stream and while that stream is actively being writen on could lead
    // to uncheck memory usage as everything will be stored in the history buffer.
    //
    // TODO: Implement a mechanism to limit the size of the history buffer by implementing a backpressure mechanism.
    #[instrument(skip(self), fields(correation = %self.context.correlation))]
    pub async fn next(&mut self) -> eyre::Result<Option<SubscriptionEvent>> {
        if self.done {
            return Ok(None);
        }

        loop {
            match std::mem::replace(&mut self.state, State::Init) {
                State::Init => {
                    match self
                        .reader
                        .read(
                            self.context,
                            &self.stream_name,
                            self.start,
                            Direction::Forward,
                            usize::MAX,
                        )
                        .await?
                    {
                        ReadStreamCompleted::StreamDeleted => {
                            self.done = true;
                            return Ok(Some(SubscriptionEvent::Unsubscribed(
                                UnsubscribeReason::Server,
                            )));
                        }

                        ReadStreamCompleted::Success(reader) => {
                            let current_revision = self
                                .index
                                .latest_revision(self.context, mikoshi_hash(&self.stream_name))
                                .await?;

                            let end = current_revision.revision().unwrap_or_default();

                            let sub = self
                                .sub
                                .subscribe_to_stream(self.context, &self.stream_name)
                                .await?;

                            self.state = State::CatchingUp(CatchingUp {
                                reader,
                                sub,
                                end,
                                history: VecDeque::new(),
                                conf: None,
                            });
                        }
                    }
                }

                State::CatchingUp(mut c) => {
                    select! {
                        outcome = c.reader.next() => {
                            match outcome {
                                Err(e) => return Err(e),
                                Ok(outcome) => if let Some(event) = outcome {
                                    self.state = State::CatchingUp(c);
                                    return Ok(Some(SubscriptionEvent::EventAppeared(event)))
                                } else {
                                    self.state = State::PlayHistory(PlayHistory { history: c.history, sub: c.sub });
                                    return Ok(Some(SubscriptionEvent::CaughtUp));
                                }
                            }
                        }

                        outcome = c.sub.next() => match outcome {
                            Err(e) => return Err(e),
                            Ok(outcome) => {
                                if let Some(event) = outcome {
                                    match event {
                                        SubscriptionEvent::EventAppeared(record) => {
                                            if record.revision <= c.end {
                                                continue;
                                            }

                                            c.history.push_back(record);
                                            self.state = State::CatchingUp(c);
                                        }

                                        SubscriptionEvent::Confirmed(conf) => {
                                            c.conf = Some(conf);
                                        }

                                        SubscriptionEvent::Unsubscribed(reason) => {
                                            self.done = true;
                                            return Ok(Some(SubscriptionEvent::Unsubscribed(reason)));
                                        }

                                        SubscriptionEvent::CaughtUp => unreachable!(),
                                    }
                                } else {
                                    self.done = true;
                                    return Ok(None);
                                }
                            }
                        }
                    }
                }

                State::PlayHistory(mut h) => {
                    if let Some(record) = h.history.pop_front() {
                        self.state = State::PlayHistory(h);
                        return Ok(Some(SubscriptionEvent::EventAppeared(record)));
                    }

                    self.state = State::Live(h.sub);
                }

                State::Live(mut sub) => {
                    if let Some(event) = sub.next().await? {
                        return Ok(Some(event));
                    }

                    self.done = true;
                    return Ok(None);
                }
            }
        }
    }
}
