use std::io;

use geth_mikoshi::IteratorIO;
use geth_mikoshi::wal::entries::EntryIter;
use geth_mikoshi::wal::WriteAheadLog;

use crate::binary::events::Event;
use crate::parse_event;

pub struct EventIter<WAL> {
    inner: EntryIter<WAL>,
}

impl<WAL: WriteAheadLog> IteratorIO for EventIter<WAL> {
    type Item = (u64, crate::RecordedEvent);

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        loop {
            if let Some(item) = self.inner.next()? {
                let event = match parse_event(&item.payload) {
                    Err(e) => {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
                    }

                    Ok(event) => event,
                };

                if let Event::RecordedEvent(event) = event.event.unwrap() {
                    return Ok(Some((item.position, crate::RecordedEvent::from(event))));
                }
            }

            return Ok(None);
        }
    }
}
