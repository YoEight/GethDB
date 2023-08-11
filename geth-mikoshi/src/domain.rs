use crate::marshalling::{get_string, put_string, variable_string_length_bytes_size};
use crate::wal::{LogEntryType, LogRecord};
use bytes::{Buf, BufMut, Bytes};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct StreamEventAppended {
    pub revision: u64,
    pub event_stream_id: String,
    pub event_id: Uuid,
    pub created: i64,
    pub event_type: String,
    pub data: Bytes,
    pub metadata: Bytes,
}

impl LogRecord for StreamEventAppended {
    fn get(mut src: Bytes) -> Self {
        let expected_version = src.get_u64_le();
        let event_stream_id = get_string(&mut src);
        let event_id = Uuid::from_u128(src.get_u128_le());
        let timestamp = src.get_i64_le();
        let event_type = get_string(&mut src);
        let data_len = src.get_u32_le() as usize;
        let data = src.copy_to_bytes(data_len as usize);
        let metadata_len = src.get_u32_le() as usize;
        let metadata = src.copy_to_bytes(metadata_len as usize);

        StreamEventAppended {
            revision: expected_version,
            event_stream_id,
            event_id,
            created: timestamp,
            event_type,
            data,
            metadata,
        }
    }

    fn put(&self, buffer: &mut bytes::BytesMut) {
        buffer.put_u64_le(self.revision);
        put_string(&self.event_stream_id, buffer);
        buffer.put_u128_le(self.event_id.as_u128());
        buffer.put_i64_le(self.created);
        put_string(&self.event_type, buffer);
        buffer.put_u32_le(self.data.len() as u32);
        buffer.put(self.data.clone());
        buffer.put_u32_le(self.metadata.len() as u32);
        buffer.put(self.metadata.clone());
    }

    fn r#type() -> LogEntryType {
        LogEntryType::UserData
    }

    fn size(&self) -> usize {
        8 // expected version 
            + variable_string_length_bytes_size(self.event_stream_id.len())
            + self.event_stream_id.len()
            + 16 // event id
            + 8 // timestamp
            + variable_string_length_bytes_size(self.event_type.len())
            + self.event_type.len()
            + 4 // data encoded length
            + self.data.len()
            + 4 // metadata encoded length
            + self.metadata.len()
    }
}
