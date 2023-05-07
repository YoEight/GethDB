use bitflags::bitflags;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

bitflags! {
    pub struct PrepareFlags: u16 {
        const NO_DATA = 0x00;
        const HAS_DATA = 0x01;
        const TRANSACTION_START = 0x02;
        const TRANSACTION_END = 0x04;
        const DELETED_STREAM = 0x08;
        const IS_COMMITTED = 0x20;
        const IS_JSON = 0x100;
    }
}

#[derive(Debug, Clone)]
pub struct PrepareLog {
    pub flags: PrepareFlags,
    pub transaction_position: u64,
    pub transaction_offset: u32,
    pub revision: u64,
    pub event_stream_id: String,
    pub event_id: Uuid,
    pub correlation_id: Uuid,
    pub created: i64,
    pub event_type: String,
    pub data: Bytes,
    pub metadata: Bytes,
}

impl PrepareLog {
    pub fn is_data_json(&self) -> bool {
        self.flags.contains(PrepareFlags::IS_JSON)
    }

    pub fn size(&self) -> usize {
        2 // prepare flags
            + 8 // transaction position
            + 4 // transaction offset
            + 8 // expected version 
            + variable_string_length_bytes_size(self.event_stream_id.len())
            + self.event_stream_id.len()
            + 16 // event id
            + 16 // correlation id
            + 8 // timestamp
            + variable_string_length_bytes_size(self.event_type.len())
            + self.event_type.len()
            + 4 // data encoded length
            + self.data.len()
            + 4 // metadata encoded length
            + self.metadata.len()
    }

    pub fn get(mut src: Bytes) -> Self {
        let flags =
            PrepareFlags::from_bits(src.get_u16_le()).expect("Invalid prepare flags parsing");
        let transaction_position = src.get_u64_le();
        let transaction_offset = src.get_u32_le();
        let expected_version = src.get_u64_le();
        let event_stream_id = get_string(&mut src);
        let event_id = Uuid::from_u128(src.get_u128_le());
        let correlation_id = Uuid::from_u128(src.get_u128_le());
        let timestamp = src.get_i64_le();
        let event_type = get_string(&mut src);
        let data_len = src.get_u32_le() as usize;
        let data = src.copy_to_bytes(data_len as usize);
        let metadata_len = src.get_u32_le() as usize;
        let metadata = src.copy_to_bytes(metadata_len as usize);

        PrepareLog {
            flags,
            transaction_offset,
            transaction_position,
            revision: expected_version,
            event_stream_id,
            event_id,
            correlation_id,
            created: timestamp,
            event_type,
            data,
            metadata,
        }
    }

    pub fn put(&self, buffer: &mut bytes::BytesMut) {
        buffer.put_u16_le(self.flags.bits());
        buffer.put_u64_le(self.transaction_position);
        buffer.put_u32_le(self.transaction_offset);
        buffer.put_u64_le(self.revision);
        put_string(&self.event_stream_id, buffer);
        buffer.put_u128_le(self.event_id.as_u128());
        buffer.put_u128_le(self.correlation_id.as_u128());
        buffer.put_i64_le(self.created);
        put_string(&self.event_type, buffer);
        buffer.put_u32_le(self.data.len() as u32);
        buffer.put(self.data.clone());
        buffer.put_u32_le(self.metadata.len() as u32);
        buffer.put(self.metadata.clone());
    }
}

fn variable_string_length_bytes_size(value: usize) -> usize {
    let mut value = value as u64;
    let mut count = 0usize;

    while value > 0x7F {
        count += 1;
        value >>= 7;
    }

    count + 1
}

fn put_string(src: &String, buf: &mut BytesMut) {
    let mut value = src.len() as u64;

    while value > 0x7F {
        buf.put_u8((value | 0x80) as u8);
        value >>= 7;
    }

    buf.put_u8(value as u8);
    buf.extend_from_slice(src.as_bytes());
}

pub fn get_string(buf: &mut Bytes) -> String {
    let mut string_len = 0u32;
    let mut shift = 0u8;

    loop {
        let current = buf.get_u8();

        string_len |= ((current & 0x7F) as u32) << shift;

        if (current & 0x80) == 0 {
            break;
        }

        shift += 7;

        if shift > (4 * 7) {
            panic!("Maximum encoding length exceeded");
        }
    }

    String::from_utf8(buf.copy_to_bytes(string_len as usize).to_vec()).unwrap()
}

#[derive(Debug, Clone, Copy)]
pub enum SystemRecordType {
    Invalid,
    Epoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemRecordFormat {
    Invalid,
    Binary,
    Json,
    Bson,
}

#[derive(Debug, Clone)]
pub struct SystemLog {
    pub created: DateTime<Utc>,
    pub kind: SystemRecordType,
    pub format: SystemRecordFormat,
    pub data: Bytes,
}

impl SystemLog {
    pub fn get(mut src: Bytes) -> Self {
        let timestamp = src.get_i64_le();
        let kind = match src.get_u8() {
            0 => SystemRecordType::Invalid,
            1 => SystemRecordType::Epoch,
            _ => panic!("Unknown system record type"),
        };

        let format = match src.get_u8() {
            0 => SystemRecordFormat::Invalid,
            1 => SystemRecordFormat::Binary,
            2 => SystemRecordFormat::Json,
            3 => SystemRecordFormat::Bson,
            _ => panic!("Unknown system record format"),
        };

        // Reserved unused data.
        let _ = src.get_i64_le();
        let data_length = src.get_u32_le() as i64;
        let data = src.copy_to_bytes(data_length as usize);

        SystemLog {
            created: Utc.timestamp_opt(timestamp, 0).unwrap(),
            kind,
            format,
            data,
        }
    }

    pub fn put(&self, buf: &mut BytesMut) {
        buf.put_i64_le(self.created.timestamp());

        match self.kind {
            SystemRecordType::Invalid => buf.put_u8(0),
            SystemRecordType::Epoch => buf.put_u8(1),
        }

        match self.format {
            SystemRecordFormat::Invalid => buf.put_u8(0),
            SystemRecordFormat::Binary => buf.put_u8(1),
            SystemRecordFormat::Json => buf.put_u8(2),
            SystemRecordFormat::Bson => buf.put_u8(3),
        }

        // Reserved unused data.
        buf.put_i64_le(0);
        buf.put_u32_le(self.data.len() as u32);
        buf.put(self.data.clone());
    }

    pub fn is_json_format(&self) -> bool {
        self.format == SystemRecordFormat::Json
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct EpochRecord {
    #[serde(rename = "EpochId")]
    pub id: Uuid,
    pub leader_instance_id: Uuid,
    #[serde(rename = "EpochPosition")]
    pub position: i64,
    #[serde(rename = "EpochNumber")]
    pub number: u64,
    #[serde(rename = "PrevEpochPosition")]
    pub previous_position: i64,
    #[serde(rename = "TimeStamp")]
    pub time: DateTime<Utc>,
}

impl EpochRecord {
    pub fn new(
        id: Uuid,
        leader_instance_id: Uuid,
        number: u64,
        position: i64,
        last_epoch_position: i64,
    ) -> Self {
        Self {
            id,
            leader_instance_id,
            position,
            number,
            previous_position: last_epoch_position,
            time: Utc::now(),
        }
    }
}
