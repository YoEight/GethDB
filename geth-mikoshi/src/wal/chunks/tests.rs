use crate::storage::InMemoryStorage;
use crate::wal::chunks::ChunkContainer;
use crate::wal::{LogEntries, LogReader, LogWriter};
use bytes::{Bytes, BytesMut};
use geth_common::{Propose, Record};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
    let container = ChunkContainer::load(storage.clone())?;
    let stream_name = "salut".to_string();
    let r#type = "foobar".to_string();
    let id = Uuid::new_v4();
    let revision = 42;
    let data = generate_bytes();
    let mut entries = LogEntries::new();
    let reader = LogReader::new(container.clone());
    let mut writer = LogWriter::load(container.clone(), BytesMut::new())?;

    entries.begin(
        stream_name.clone(),
        revision,
        vec![Propose {
            id,
            r#type: r#type.clone(),
            data: data.clone(),
        }],
    );
    writer.append(&mut entries)?;

    let record: Record = reader.read_at(0)?.into();

    assert_eq!(revision, record.revision);
    assert_eq!(stream_name.len(), record.stream_name.len());
    assert_eq!(stream_name, record.stream_name);
    assert_eq!(r#type, record.r#type);

    Ok(())
}
