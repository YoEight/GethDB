use crate::backend::esdb::index::StreamIndex;
use crate::backend::esdb::manager::{ChunkManager, FullScan};
use crate::backend::esdb::types::{
    Checkpoint, Chunk, ChunkFooter, ChunkHeader, FooterFlags, PrepareFlags, PrepareLog,
    ProposedEvent, CHUNK_FILE_SIZE, CHUNK_FOOTER_SIZE, CHUNK_HEADER_SIZE, CHUNK_SIZE,
};
use crate::backend::esdb::utils::{chunk_filename_from, list_chunk_files, md5_hash_chunk_file};
use crate::backend::Backend;
use crate::{Entry, MikoshiStream};
use byteorder::{LittleEndian, WriteBytesExt};
use bytes::BytesMut;
use chrono::Utc;
use geth_common::{Direction, ExpectedRevision, Position, Propose, Revision, WriteResult};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, error};
use uuid::Uuid;

mod index;
// mod manager;
mod asynchronous;
mod manager;
pub mod parsing;
mod synchronous;
pub mod types;
mod utils;

pub struct EsdbBackend {
    revisions: HashMap<String, u64>,
    stream_index: StreamIndex,
    manager: ChunkManager,
    buffer: BytesMut,
}

impl EsdbBackend {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let manager = ChunkManager::new(path)?;

        Ok(Self {
            stream_index: StreamIndex::new(),
            revisions: Default::default(),
            buffer: BytesMut::new(),
            manager,
        })
    }
}

fn chunk_idx_from_logical_position(log_position: i64) -> usize {
    let mut chunk_idx = 0usize;

    while (chunk_idx + 1) * CHUNK_SIZE <= (log_position as usize) {
        chunk_idx += 1;
    }

    chunk_idx
}

impl Backend for EsdbBackend {
    fn append(
        &mut self,
        stream_name: String,
        _expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> io::Result<WriteResult> {
        let mut revision = self.stream_index.stream_current_revision(&stream_name);
        let mut log_position = self.manager.log_position()?;
        debug!("Current log position: {}", log_position);

        for propose in events {
            let correlation_id = Uuid::new_v4();
            let record_position = log_position;
            let mut flags: PrepareFlags = PrepareFlags::HAS_DATA
                | PrepareFlags::TRANSACTION_START
                | PrepareFlags::TRANSACTION_END
                | PrepareFlags::IS_COMMITTED
                | PrepareFlags::IS_JSON;

            // TODO - Implement proper weird Windows EPOCH time. I already done the work on Rust TCP client of old.
            // I know the transaction log doesn't support UNIX EPOCH time but that weird
            // Windows epoch that nobody uses besides windows.
            let timestamp = Utc::now().timestamp();
            let prepare = PrepareLog {
                flags,
                transaction_position: log_position as i64,
                transaction_offset: 0,
                expected_version: revision as i64,
                event_stream_id: stream_name.clone(),
                event_id: propose.id,
                correlation_id,
                timestamp,
                event_type: propose.r#type,
                data: bytes::Bytes::from(propose.data.to_vec()),
                metadata: bytes::Bytes::from(Vec::<u8>::default()),
            };

            log_position = self.manager.write_prepare_record(prepare)?;

            self.stream_index.index(&stream_name, record_position);

            revision += 1;
        }

        self.revisions.insert(stream_name, revision);
        self.manager.flush()?;

        Ok(WriteResult {
            next_expected_version: ExpectedRevision::Revision(revision),
            position: Position(log_position as u64),
        })
    }

    fn read(
        &mut self,
        stream_name: String,
        starting: Revision<u64>,
        direction: Direction,
    ) -> io::Result<MikoshiStream> {
        // TODO - Implement an async version of this code!
        // TODO - Implement backward read.

        if direction == Direction::Backward {
            return Ok(MikoshiStream::empty());
        }

        let mut entries = Vec::new();
        let end_log_position = self.manager.writer_checkpoint()? as u64;

        let chunks = self
            .manager
            .chunks()
            .iter()
            .map(|c| c.path.file_name().unwrap().to_string_lossy().to_string())
            .collect();

        let mut full_scan = FullScan::new(
            self.buffer.clone(),
            self.manager.root(),
            chunks,
            end_log_position,
        );

        while let Some(entry) = full_scan.next()? {
            if entry.stream_name != stream_name || starting.is_greater_than(entry.revision) {
                continue;
            }

            entries.push(entry);
        }

        return Ok(MikoshiStream::from_vec(entries));
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use bytes::Bytes;
    use geth_common::{Direction, ExpectedRevision, Propose, Revision};
    use uuid::Uuid;

    use crate::backend::Backend;

    use super::EsdbBackend;

    #[tokio::test]
    async fn test_write_read() -> eyre::Result<()> {
        let mut backend = EsdbBackend::new("./test-geth")?;
        let mut proposes = Vec::new();

        proposes.push(Propose {
            id: Uuid::new_v4(),
            r#type: "language-selected".to_string(),
            data: Bytes::from(serde_json::to_vec(&serde_json::json!({
                "is_rust_good": true
            }))?),
        });

        proposes.push(Propose {
            id: Uuid::new_v4(),
            r#type: "purpose-of-life".to_string(),
            data: Bytes::from(serde_json::to_vec(&serde_json::json!({
                "answer": 42
            }))?),
        });

        let input = proposes.clone();
        let result = backend.append("foobar".to_string(), ExpectedRevision::Any, proposes)?;

        println!("Write result: {:?}", result);

        let mut stream = backend.read("foobar".to_string(), Revision::Start, Direction::Forward)?;
        let mut idx = 0usize;

        while let Some(record) = stream.next().await? {
            assert_eq!(input[idx].id, record.id);
            assert_eq!(input[idx].r#type, record.r#type);
            assert_eq!(input[idx].data, record.data);

            idx += 1;
        }

        Ok(())
    }
}
