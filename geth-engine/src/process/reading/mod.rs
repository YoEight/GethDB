mod client;
mod proc;

use bytes::Buf;
pub use client::ReaderClient;
use geth_common::{ContentType, Position, Record};
use geth_mikoshi::wal::LogEntry;
pub use proc::run;
use uuid::Uuid;

pub fn record_try_from(mut entry: LogEntry) -> eyre::Result<Record> {
    let revision = entry.payload.get_u64_le();
    let stream_name_len = entry.payload.get_u16_le() as usize;
    let stream_name = unsafe {
        String::from_utf8_unchecked(entry.payload.copy_to_bytes(stream_name_len).to_vec())
    };

    let id = Uuid::from_u128_le(entry.payload.get_u128_le());
    let content_type = entry.payload.get_u32_le() as i32;
    let class_len = entry.payload.get_u16_le() as usize;
    let class =
        unsafe { String::from_utf8_unchecked(entry.payload.copy_to_bytes(class_len).to_vec()) };
    entry.payload.advance(size_of::<u32>()); // skip the payload size

    Ok(Record {
        id,
        content_type: ContentType::try_from(content_type)?,
        stream_name,
        class,
        position: Position(entry.position),
        revision,
        data: entry.payload,
    })
}
