use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use prost::Message;

use geth_common::Propose;

pub struct AppendProposes<'a, I> {
    events: I,
    buffer: &'a mut BytesMut,
    stream_name: String,
    created: i64,
    revision: u64,
}

impl<'a, I> AppendProposes<'a, I>
where
    I: Iterator<Item = Propose>,
{
    pub fn new(
        stream_name: String,
        created: DateTime<Utc>,
        start_revision: u64,
        buffer: &'a mut BytesMut,
        events: I,
    ) -> Self {
        Self {
            events,
            buffer,
            stream_name,
            revision: start_revision,
            created: created.timestamp(),
        }
    }
}

impl<'a, I> Iterator for AppendProposes<'a, I>
where
    I: Iterator<Item = Propose>,
{
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        let propose = self.events.next()?;
        let event = crate::binary::events::RecordedEvent {
            id: propose.id.into(),
            revision: self.revision,
            stream_name: self.stream_name.clone(),
            class: propose.r#type,
            created: self.created,
            data: propose.data,
            metadata: Default::default(),
        };

        self.revision += 1;
        let event = crate::binary::events::Events {
            event: Some(crate::binary::events::Event::RecordedEvent(event)),
        };

        event.encode(&mut self.buffer).unwrap();
        Some(self.buffer.split().freeze())
    }
}
