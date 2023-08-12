use crate::domain::StreamEventAppended;
use crate::storage::InMemoryStorage;
use crate::wal::chunks::ChunkBasedWAL;
use crate::wal::{LogEntryType, WriteAheadLog};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
struct Foobar {
    value: u32,
}

#[test]
fn test_wal_chunk_iso() -> eyre::Result<()> {
    let storage = InMemoryStorage::new();
    let mut wal = ChunkBasedWAL::load(storage)?;
    let data = serde_json::to_vec(&Foobar { value: 42 })?;
    let event_id = Uuid::new_v4();
    let transaction_id = Uuid::new_v4();
    let transaction_offset = 0;
    let created = Utc::now().timestamp();

    wal.append(&[StreamEventAppended {
        revision: 3,
        event_stream_id: "foobar".to_string(),
        transaction_id,
        transaction_offset,
        event_id,
        created,
        event_type: "created".to_string(),
        data: data.into(),
        metadata: Default::default(),
    }])?;

    let entry = wal.read_at(0)?;

    assert_eq!(LogEntryType::UserData, entry.r#type);

    let event = entry.unmarshall::<StreamEventAppended>();

    assert_eq!(3, event.revision);
    assert_eq!("foobar", event.event_stream_id);
    assert_eq!(transaction_offset, event.transaction_offset);
    assert_eq!(transaction_id, event.transaction_id);
    assert_eq!(event_id, event.event_id);
    assert_eq!(created, event.created);
    assert_eq!("created", event.event_type);

    let actual = serde_json::from_slice::<Foobar>(event.data.as_ref())?;

    assert_eq!(42, actual.value);

    Ok(())
}
