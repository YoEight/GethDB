use crate::constants::{CHUNK_FOOTER_SIZE, CHUNK_HEADER_SIZE, CHUNK_SIZE};
use crate::hashing::mikoshi_hash;
use crate::index::{Lsm, LsmSettings};
use crate::storage::{FileCategory, FileId, Storage};
use crate::wal::chunk::{Chunk, ChunkInfo};
use crate::wal::footer::{ChunkFooter, FooterFlags};
use crate::wal::header::ChunkHeader;
use crate::wal::record::{PrepareFlags, PrepareLog};
use crate::wal::Proposition;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::Utc;
use geth_common::{ExpectedRevision, Propose, WriteResult};
use md5::Digest;
use sha2::Sha512;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::io;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

#[derive(Copy, Clone, Debug)]
pub struct Chunks;

impl FileCategory for Chunks {
    type Item = ChunkInfo;

    fn parse(&self, name: &str) -> Option<Self::Item> {
        ChunkInfo::from_chunk_filename(name)
    }
}

struct State {
    chunks: Vec<Chunk>,
    writer: u64,
}

impl State {
    fn ongoing_chunk(&self) -> Chunk {
        self.chunks.last().cloned().unwrap()
    }

    fn new_chunk(&mut self) -> Chunk {
        let new = self.ongoing_chunk().next_chunk();
        self.chunks.push(new.clone());

        new
    }

    fn chunk_mut(&mut self, idx: usize) -> &mut Chunk {
        &mut self.chunks[idx]
    }

    fn ongoing_chunk_mut(&mut self) -> &mut Chunk {
        self.chunk_mut(self.chunks.len() - 1)
    }
}

#[derive(Clone)]
pub struct ChunkManager<S> {
    buffer: BytesMut,
    storage: S,
    state: Arc<RwLock<State>>,
}

impl<S> ChunkManager<S>
where
    S: Storage + 'static,
{
    pub fn load(storage: S) -> io::Result<Self> {
        let mut sorted_chunks = BTreeMap::<usize, ChunkInfo>::new();

        for info in storage.list(Chunks)? {
            if let Some(chunk) = sorted_chunks.get_mut(&info.seq_num) {
                if chunk.version < info.version {
                    *chunk = info;
                }
            } else {
                sorted_chunks.insert(info.seq_num, info);
            }
        }

        let mut chunks = Vec::new();
        for info in sorted_chunks.into_values() {
            let header = storage.read_from(info.file_id(), 0, CHUNK_HEADER_SIZE)?;
            let header = ChunkHeader::get(header);
            let footer = storage.read_from(
                info.file_id(),
                (CHUNK_SIZE - CHUNK_FOOTER_SIZE) as u64,
                CHUNK_FOOTER_SIZE,
            )?;
            let footer = ChunkFooter::get(footer);
            let chunk = Chunk {
                info,
                header,
                footer,
            };

            chunks.push(chunk);
        }

        let mut writer = 0u64;
        if !storage.exists(FileId::writer_chk())? {
            flush_writer_chk(&storage, writer)?;
        } else {
            writer = storage.read_from(FileId::writer_chk(), 0, 8)?.get_u64_le();
        }

        Ok(Self {
            buffer: BytesMut::new(),
            storage,
            state: Arc::new(RwLock::new(State { chunks, writer })),
        })
    }

    pub fn append(
        &self,
        stream_name: String,
        expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> io::Result<Proposition> {
        let stream_key = mikoshi_hash(stream_name.as_str());
        // TODO - cache that value when possible.
        let mut state = self.state.write().unwrap();
        let batch_size = events.len();
        let correlation_id = Uuid::new_v4();
        let transaction_position = state.writer;
        let mut expected_revision = expected.raw();
        let mut buffer = self.buffer.clone();
        let mut chunk = state.ongoing_chunk();
        let mut transient_log_position = transaction_position;
        let mut before_writing_log_position = transient_log_position;
        let mut proposition = Proposition::new(stream_key, transaction_position);

        for (offset, propose) in events.into_iter().enumerate() {
            proposition.claims.push(transaction_position);

            let mut flags: PrepareFlags =
                PrepareFlags::HAS_DATA | PrepareFlags::IS_COMMITTED | PrepareFlags::IS_JSON;

            if offset == 0 {
                flags |= PrepareFlags::TRANSACTION_START;
            }

            if offset == batch_size - 1 {
                flags |= PrepareFlags::TRANSACTION_END;
            }

            let prepare = PrepareLog {
                flags,
                transaction_position,
                transaction_offset: offset as u32,
                expected_revision,
                event_stream_id: stream_name.clone(),
                event_id: propose.id,
                correlation_id,
                created: Utc::now(),
                event_type: propose.r#type,
                data: propose.data,
                metadata: bytes::Bytes::default(),
            };

            let log_record_size = 4 // pre record size
                    + 8 // log position
                    + prepare.size()
                    + 4; // post record size

            let projected_logical_position = transient_log_position + log_record_size as u64;

            if chunk.contains_log_position(projected_logical_position) {
                buffer.put_u32_le(log_record_size as u32);
                buffer.put_u64_le(transient_log_position);
                prepare.put(&mut buffer);
                buffer.put_u32_le(log_record_size as u32);

                transient_log_position += projected_logical_position;

                continue;
            }

            // Chunk is full and we need to flush previous data we accumulated. We also create a new
            // chunk for next writes.
            if !buffer.is_empty() {
                let end_log_position = transient_log_position + buffer.len() as u64;
                let local_offset = chunk.raw_position(before_writing_log_position);
                let physical_data_size =
                    chunk.raw_position(end_log_position) as usize - CHUNK_HEADER_SIZE;
                let footer = ChunkFooter {
                    flags: FooterFlags::IS_COMPLETED,
                    physical_data_size,
                    logical_data_size: physical_data_size,
                    hash: Default::default(),
                };

                self.storage
                    .write_to(chunk.file_type(), local_offset, buffer.split().freeze())?;

                footer.put(&mut buffer);
                state.ongoing_chunk_mut().footer = Some(footer);

                self.storage.write_to(
                    chunk.file_type(),
                    (CHUNK_SIZE - CHUNK_FOOTER_SIZE) as u64,
                    buffer.split().freeze(),
                )?;

                chunk = state.new_chunk();
                before_writing_log_position = end_log_position;
                transient_log_position = end_log_position;
            }

            expected_revision += 1;
        }

        let local_offset = chunk.raw_position(before_writing_log_position);
        self.storage
            .write_to(chunk.file_type(), local_offset, buffer.split().freeze())?;

        state.writer = transient_log_position;
        flush_writer_chk(&self.storage, state.writer)?;

        Ok(proposition)
    }
}

fn flush_writer_chk<S>(storage: &S, log_pos: u64) -> io::Result<()>
where
    S: Storage,
{
    storage.write_to(
        FileId::writer_chk(),
        0,
        Bytes::copy_from_slice(log_pos.to_le_bytes().as_slice()),
    )
}
