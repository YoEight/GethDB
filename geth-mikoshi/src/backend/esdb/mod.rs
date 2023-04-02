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

// mod manager;
mod asynchronous;
pub mod parsing;
mod synchronous;
pub mod types;
mod utils;

pub use synchronous::BlockingEsdbBackend;

#[cfg(test)]
mod tests {
    use std::io;

    use bytes::Bytes;
    use geth_common::{Direction, ExpectedRevision, Propose, Revision};
    use uuid::Uuid;

    use crate::backend::esdb::synchronous::BlockingEsdbBackend;
    use crate::backend::Backend;

    #[tokio::test]
    async fn test_write_read() -> eyre::Result<()> {
        let mut backend = BlockingEsdbBackend::new("./test-geth")?;
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

        while let Some(record) = stream.next()? {
            assert_eq!(input[idx].id, record.id);
            assert_eq!(input[idx].r#type, record.r#type);
            assert_eq!(input[idx].data, record.data);

            idx += 1;
        }

        Ok(())
    }
}
