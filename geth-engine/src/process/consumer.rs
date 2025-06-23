use std::{collections::VecDeque, fmt::Display, usize};

use geth_common::{
    Direction, ReadStreamCompleted, Record, Revision, SubscriptionEvent, UnsubscribeReason,
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

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::Init => write!(f, "Init"),
            State::CatchingUp(_) => write!(f, "CatchingUp"),
            State::PlayHistory(_) => write!(f, "PlayHistory"),
            State::Live(_) => write!(f, "Live"),
        }
    }
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
    #[instrument(skip(self), fields(correation = %self.context.correlation, stream_name = self.stream_name, state = %self.state))]
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

                            let mut sub = self
                                .sub
                                .subscribe_to_stream(self.context, &self.stream_name)
                                .await?;

                            if let Some(SubscriptionEvent::Confirmed(conf)) = sub.next().await? {
                                self.state = State::CatchingUp(CatchingUp {
                                    reader,
                                    sub,
                                    end,
                                    history: VecDeque::new(),
                                });

                                return Ok(Some(SubscriptionEvent::Confirmed(conf)));
                            }

                            self.done = true;
                            eyre::bail!("subscription was not confirmed");
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
                                    if c.history.is_empty() {
                                        self.state = State::Live(c.sub);
                                    } else {
                                        self.state = State::PlayHistory(PlayHistory { history: c.history, sub: c.sub });
                                    }

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

                                        SubscriptionEvent::Unsubscribed(reason) => {
                                            self.done = true;
                                            return Ok(Some(SubscriptionEvent::Unsubscribed(reason)));
                                        }

                                        SubscriptionEvent::CaughtUp | SubscriptionEvent::Confirmed(_) => unreachable!(),
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
