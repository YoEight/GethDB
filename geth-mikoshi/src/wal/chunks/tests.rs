use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::storage::InMemoryStorage;
use crate::wal::chunks::ChunkBasedWAL;
use crate::wal::{LogEntries, WriteAheadLog};

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
    let mut wal = ChunkBasedWAL::load(storage)?;
    let data = generate_bytes();
    buffer.put_u32_le(data.len() as u32);
    buffer.extend_from_slice(&data);
    let mut entries = LogEntries::new(
        Bytes::from_static(b"salut"),
        0,
        false,
        buffer.split().freeze(),
    );

    wal.append(&mut entries)?;

    let entry = wal.read_at(0)?;

    assert_eq!(data, entry.payload);

    Ok(())
}
