use crate::storage::InMemoryStorage;
use crate::wal::chunks::{ChunkBasedWAL, ChunkContainer};
use crate::wal::{LogEntries, WriteAheadLog};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

#[derive(Deserialize, Serialize)]
struct Foobar {
    value: u32,
}

fn generate_bytes() -> Bytes {
    let mut bytes = Vec::new();

    for b in 0..=127u8 {
        bytes.push(b);
    }

    Bytes::from(bytes)
}

#[test]
fn test_wal_chunk_iso() -> eyre::Result<()> {
    let mut buffer = BytesMut::new();
    let storage = InMemoryStorage::new();
    let container = ChunkContainer::load(storage.clone())?;
    let mut wal = ChunkBasedWAL::new(container)?;
    let stream_name = Bytes::from_static(b"salut");
    let revision = 42;
    let data = generate_bytes();
    buffer.put_u32_le(data.len() as u32);
    buffer.extend_from_slice(&data);
    let mut entries = LogEntries::new(buffer);

    entries.begin(stream_name.clone(), revision, data.clone());
    wal.append(&mut entries)?;

    let mut entry = wal.read_at(0)?;
    let actual_revision = entry.payload.get_u64_le();
    let str_len = entry.payload.get_u16_le() as usize;
    let actual_stream_name = entry.payload.copy_to_bytes(str_len);
    let payload_size = entry.payload.get_u32_le() as usize;
    let payload = entry.payload.copy_to_bytes(payload_size);

    assert_eq!(revision, actual_revision);
    assert_eq!(stream_name.len(), actual_stream_name.len());
    assert_eq!(stream_name, actual_stream_name);
    assert_eq!(data, payload);

    Ok(())
}
