use std::{collections::VecDeque, fmt::Display};

use geth_common::{Direction, ReadStreamCompleted, Record, Revision, SubscriptionEvent};
use geth_mikoshi::hashing::mikoshi_hash;
use tokio::select;
use tracing::instrument;

use crate::{process::subscription, reading, ManagerClient, RequestContext};

#[derive(Clone, Copy, Debug)]
enum State {
    Init,
    CatchingUp,
    PlayHistory,
    Live,
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::Init => write!(f, "init"),
            State::CatchingUp => write!(f, "catching-up"),
            State::PlayHistory => write!(f, "play-history"),
            State::Live => write!(f, "live"),
        }
    }
}

pub struct Consumer {
    context: RequestContext,
    state: State,
    done: bool,
    stream_name: String,
    end: u64,
    history: VecDeque<Record>,
    reader: reading::Streaming,
    sub: subscription::Streaming,
}

pub enum ConsumerResult {
    Success(Consumer),
    StreamDeleted,
}

pub async fn start_consumer(
    context: RequestContext,
    stream_name: String,
    start: Revision<u64>,
    client: ManagerClient,
) -> eyre::Result<ConsumerResult> {
    let index = client.new_index_client().await?;
    let reader = client.new_reader_client().await?;
    let sub = client.new_subscription_client().await?;

    let result = reader
        .read(context, &stream_name, start, Direction::Forward, usize::MAX)
        .await?;

    let reader = match result {
        ReadStreamCompleted::StreamDeleted => return Ok(ConsumerResult::StreamDeleted),
        ReadStreamCompleted::Success(s) => s,
    };

    let result = index
        .latest_revision(context, mikoshi_hash(&stream_name))
        .await?;

    if result.is_deleted() {
        return Ok(ConsumerResult::StreamDeleted);
    }

    let end = result.revision().unwrap_or_default();
    let sub = sub.subscribe_to_stream(context, &stream_name).await?;

    Ok(ConsumerResult::Success(Consumer {
        context,
        state: State::Init,
        done: false,
        history: VecDeque::new(),
        stream_name,
        end,
        reader,
        sub,
    }))
}

impl Consumer {
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
            match self.state {
                State::Init => {
                    if let Some(SubscriptionEvent::Confirmed(conf)) = self.sub.next().await? {
                        self.state = State::CatchingUp;
                        return Ok(Some(SubscriptionEvent::Confirmed(conf)));
                    }

                    self.done = true;
                    eyre::bail!("subscription was not confirmed");
                }

                State::CatchingUp => {
                    select! {
                        outcome = self.reader.next() => {
                            match outcome {
                                Err(e) => return Err(e),
                                Ok(outcome) => if let Some(event) = outcome {
                                    tracing::debug!("event read from the reader");
                                    return Ok(Some(SubscriptionEvent::EventAppeared(event)))
                                } else {
                                    if self.history.is_empty() {
                                        self.state = State::Live;
                                    } else {
                                        self.state = State::PlayHistory;
                                    }

                                    return Ok(Some(SubscriptionEvent::CaughtUp));
                                }
                            }
                        }

                        outcome = self.sub.next() => match outcome {
                            Err(e) => return Err(e),
                            Ok(outcome) => {
                                if let Some(event) = outcome {
                                    match event {
                                        SubscriptionEvent::EventAppeared(record) => {
                                            if record.revision < self.end {
                                                continue;
                                            }

                                            self.end = record.revision;
                                            self.history.push_back(record);
                                        }

                                        SubscriptionEvent::Unsubscribed(reason) => {
                                            self.done = true;
                                            return Ok(Some(SubscriptionEvent::Unsubscribed(reason)));
                                        }

                                        SubscriptionEvent::Notification(n) => return Ok(Some(SubscriptionEvent::Notification(n))),

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

                State::PlayHistory => {
                    if let Some(record) = self.history.pop_front() {
                        self.end = record.revision;
                        return Ok(Some(SubscriptionEvent::EventAppeared(record)));
                    }

                    self.state = State::Live;
                }

                State::Live => {
                    if let Some(event) = self.sub.next().await? {
                        if let SubscriptionEvent::EventAppeared(temp) = &event {
                            if temp.revision < self.end {
                                continue;
                            }
                        }

                        return Ok(Some(event));
                    }

                    self.done = true;
                    return Ok(None);
                }
            }
        }
    }
}
