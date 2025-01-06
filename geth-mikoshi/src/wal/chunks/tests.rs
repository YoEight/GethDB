use std::vec;

use crate::storage::InMemoryStorage;
use crate::wal::chunks::ChunkContainer;
use crate::wal::{LogEntries, LogReader, LogWriter};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

struct RawEntries {
    entries: vec::IntoIter<Bytes>,
    current: Option<Bytes>,
}

impl RawEntries {
    fn new(entries: Vec<Bytes>) -> Self {
        Self {
            entries: entries.into_iter(),
            current: None,
        }
    }
}

impl LogEntries for RawEntries {
    fn move_next(&mut self) -> bool {
        if let Some(entry) = self.entries.next() {
            self.current = Some(entry);
            return true;
        }

        false
    }

    fn current_entry_size(&self) -> usize {
        self.current.as_ref().unwrap().len()
    }

    fn write_current_entry(&mut self, buffer: &mut BytesMut, _: u64) {
        buffer.extend_from_slice(self.current.as_ref().unwrap());
    }
}

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
    let data = generate_bytes();
    let mut entries = RawEntries::new(vec![data.clone()]);
    let reader = LogReader::new(container.clone());
    let mut writer = LogWriter::load(container.clone(), BytesMut::new())?;

    writer.append(&mut entries)?;

    let entry = reader.read_at(0)?;

    assert_eq!(0, entry.position);
    assert_eq!(0, entry.r#type);
    assert_eq!(data, entry.payload);

    Ok(())
}
