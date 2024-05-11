use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::storage::InMemoryStorage;
use crate::wal::chunks::ChunkBasedWAL;
use crate::wal::WriteAheadLog;

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
    let storage = InMemoryStorage::new();
    let mut wal = ChunkBasedWAL::load(storage)?;
    let data = generate_bytes();

    wal.append(vec![data.clone()])?;

    let entry = wal.read_at(0)?;

    assert_eq!(data, entry.payload);

    Ok(())
}
