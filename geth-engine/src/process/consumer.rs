use std::{cmp::max, collections::VecDeque, fmt::Display};

use geth_common::{
    Direction, ReadStreamCompleted, Record, Revision, SubscriptionEvent, UnsubscribeReason,
};
use geth_mikoshi::hashing::mikoshi_hash;
use tokio::select;
use tracing::instrument;

use crate::{
    IndexClient, ManagerClient, ReaderClient, RequestContext,
    process::subscription::{self, SubscriptionClient},
    reading,
};

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
    reader: ReaderClient,
    index: IndexClient,
    sub: SubscriptionClient,
    start: Revision<u64>,
    reader_streaming: reading::Streaming,
    sub_streaming: subscription::Streaming,
}

#[allow(clippy::large_enum_variant)]
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

    let result = index
        .latest_revision(context, mikoshi_hash(&stream_name))
        .await?;

    if result.is_deleted() {
        return Ok(ConsumerResult::StreamDeleted);
    }

    let end = result.revision().unwrap_or_default();

    Ok(ConsumerResult::Success(Consumer {
        context,
        state: State::Init,
        done: false,
        history: VecDeque::new(),
        stream_name,
        end,
        reader,
        index,
        sub,
        start,
        reader_streaming: reading::Streaming::empty(),
        sub_streaming: subscription::Streaming::empty(),
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
                    let result = self
                        .reader
                        .read(
                            self.context,
                            &self.stream_name,
                            self.start,
                            Direction::Forward,
                            usize::MAX,
                        )
                        .await?;

                    match result {
                        ReadStreamCompleted::StreamDeleted => {
                            tracing::error!("stream got deleted while streaming");
                            return Ok(Some(SubscriptionEvent::Unsubscribed(
                                UnsubscribeReason::Server,
                            )));
                        }

                        ReadStreamCompleted::Success(r) => {
                            self.reader_streaming = r;
                        }
                    };

                    let result = self
                        .index
                        .latest_revision(self.context, mikoshi_hash(&self.stream_name))
                        .await?;

                    if result.is_deleted() {
                        tracing::error!("stream got deleted while streaming");
                        return Ok(Some(SubscriptionEvent::Unsubscribed(
                            UnsubscribeReason::Server,
                        )));
                    }

                    self.end = result.revision().unwrap_or_default();

                    let mut sub_streaming = self
                        .sub
                        .subscribe_to_stream(self.context, &self.stream_name)
                        .await?;

                    if let Some(SubscriptionEvent::Confirmed(conf)) = sub_streaming.next().await? {
                        self.state = State::CatchingUp;
                        self.sub_streaming = sub_streaming;
                        return Ok(Some(SubscriptionEvent::Confirmed(conf)));
                    }

                    self.done = true;
                    eyre::bail!("subscription was not confirmed");
                }

                State::CatchingUp => {
                    select! {
                        outcome = self.reader_streaming.next() => {
                            match outcome {
                                Err(e) => return Err(e),
                                Ok(outcome) => if let Some(event) = outcome {
                                    self.end = max(self.end, event.revision);
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

                        outcome = self.sub_streaming.next() => match outcome {
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
                        if record.revision < self.end {
                            continue;
                        }

                        self.end = record.revision;
                        return Ok(Some(SubscriptionEvent::EventAppeared(record)));
                    }

                    self.state = State::Live;
                }

                State::Live => {
                    if let Some(event) = self.sub_streaming.next().await? {
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
