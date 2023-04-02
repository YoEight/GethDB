use crate::backend::esdb::synchronous::fs::{
    create_new_chunk_file, list_chunk_files, load_chunk, md5_hash_chunk_file, open_chunk_file,
};
use crate::backend::esdb::synchronous::query::full_scan::FullScan;
use crate::backend::esdb::types::{
    Checkpoint, Chunk, ChunkFooter, ChunkHeader, ChunkInfo, ChunkManager, FileType, FooterFlags,
    PrepareFlags, PrepareLog, CHUNK_FILE_SIZE, CHUNK_FOOTER_SIZE, CHUNK_HEADER_SIZE, CHUNK_SIZE,
};
use crate::backend::esdb::utils::chunk_filename_from;
use crate::backend::Backend;
use crate::{BoxedSyncMikoshiStream, EmptyMikoshiStream, Entry, MikoshiStream, SyncMikoshiStream};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::Utc;
use geth_common::{Direction, ExpectedRevision, Position, Propose, Revision, WriteResult};
use std::collections::HashMap;
use std::fs::{read_dir, File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Index;
use std::path::{Path, PathBuf};
use tracing::debug;
use uuid::Uuid;

pub struct BlockingEsdbBackend {
    root: PathBuf,
    manager: ChunkManager,
    writer: Checkpoint,
    epoch: Checkpoint,
    buffer: BytesMut,
}

impl BlockingEsdbBackend {
    pub fn new(path: impl AsRef<Path>) -> eyre::Result<Self> {
        let root = PathBuf::from(path.as_ref());
        let mut buffer = BytesMut::new();
        let mut chunk_count = 0usize;

        // Configuration
        std::fs::create_dir_all(root.as_path())?;

        // writer checkpoint.
        let mut writer = Checkpoint::new(root.join("writer.chk"))?;
        // epoch checkpoint.
        let mut epoch = Checkpoint::new(root.join("epoch.chk"))?;

        let chunk_info = list_chunk_files(root.as_path())?;
        let mut chunks = Vec::new();

        if chunk_info.is_empty() {
            let new_chunk = Chunk::new(0);
            create_new_chunk_file(&root, &new_chunk)?;
            chunks.push(new_chunk);
        } else {
            for info in chunk_info {
                let chunk = load_chunk(&mut buffer, &root, info)?;

                chunk_count = chunk.header.chunk_end_number as usize + 1;
                chunks.push(chunk);
            }
        }

        let manager = ChunkManager {
            count: chunk_count,
            chunks,
            writer: writer.read()? as u64,
            epoch: epoch.read()? as u64,
        };

        Ok(Self {
            writer,
            epoch,
            root,
            manager,
            buffer,
        })
    }
}

impl Backend for BlockingEsdbBackend {
    fn append(
        &mut self,
        stream_name: String,
        expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> eyre::Result<WriteResult> {
        let mut ongoing_chunk = self.manager.ongoing_chunk();
        let mut ongoing_chunk_file =
            open_chunk_file(&self.root, &ongoing_chunk, self.manager.writer)?;
        let transaction_position = self.manager.writer as i64;

        // TODO - Implement proper stream revision tracking;
        // TODO - Implement optimistic concurrency check with the expected version parameter.
        let mut revision = 0u64;

        for (offset, propose) in events.into_iter().enumerate() {
            let correlation_id = Uuid::new_v4();
            let record_position = self.manager.writer;

            // TODO - See how flags are set in implicit transactions.
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
                transaction_position,
                transaction_offset: offset as i32,
                expected_version: revision as i64,
                event_stream_id: stream_name.clone(),
                event_id: propose.id,
                correlation_id,
                timestamp,
                event_type: propose.r#type,
                data: propose.data,
                metadata: bytes::Bytes::from(Vec::<u8>::default()),
            };

            // Record Header stuff
            self.buffer.put_u8(0);
            self.buffer.put_u8(1);
            self.buffer.put_u64_le(record_position);
            prepare.write(1u8, &mut self.buffer);

            let record = self.buffer.split().freeze();
            let size = record.len();

            self.buffer.put_i32_le(size as i32);
            self.buffer.put(record);
            self.buffer.put_i32_le(size as i32);

            let record = self.buffer.split().freeze();

            self.manager.writer += record.len() as u64;

            loop {
                if ongoing_chunk.contains_log_position(self.manager.writer) {
                    ongoing_chunk_file.write_all(record.as_ref())?;
                    break;
                } else {
                    let new_chunk = self.manager.new_chunk();
                    let new_file = create_new_chunk_file(&self.root, &new_chunk)?;
                    let footer = ongoing_chunk.complete(&mut self.buffer, record_position);
                    let content = self.buffer.split().freeze();

                    ongoing_chunk_file.seek(SeekFrom::End(-(CHUNK_FOOTER_SIZE as i64)))?;
                    ongoing_chunk_file.write_all(content.as_ref())?;

                    debug!("Start computing MD5 hash of '{:?}'...", ongoing_chunk_file);
                    ongoing_chunk
                        .update_hash(md5_hash_chunk_file(&mut ongoing_chunk_file, footer)?);
                    debug!("Complete");

                    self.manager.update_chunk(ongoing_chunk);
                    ongoing_chunk = new_chunk;
                    ongoing_chunk_file = new_file;
                }
            }

            revision += 1;
        }

        self.writer.write(self.manager.writer as i64)?;
        self.writer.flush()?;

        Ok(WriteResult {
            next_expected_version: ExpectedRevision::Revision(revision),
            position: Position(self.manager.writer),
        })
    }

    fn read(
        &mut self,
        stream_name: String,
        starting: Revision<u64>,
        direction: Direction,
    ) -> eyre::Result<BoxedSyncMikoshiStream> {
        // TODO - Implement backward read.

        if direction == Direction::Backward {
            return Ok(Box::new(EmptyMikoshiStream));
        }

        let mut full_scan = FullScan::new(
            self.buffer.clone(),
            self.root.clone(),
            self.manager.chunks.clone(),
            self.manager.writer as u64,
        );

        return Ok(Box::new(ReadStream {
            stream_name,
            starting,
            inner: full_scan,
        }));
    }
}

struct ReadStream {
    stream_name: String,
    starting: Revision<u64>,
    inner: FullScan,
}

impl SyncMikoshiStream for ReadStream {
    fn next(&mut self) -> eyre::Result<Option<Entry>> {
        while let Some(entry) = self.inner.next()? {
            if entry.stream_name != self.stream_name
                || self.starting.is_greater_than(entry.revision)
            {
                continue;
            }

            return Ok(Some(entry));
        }

        Ok(None)
    }
}
