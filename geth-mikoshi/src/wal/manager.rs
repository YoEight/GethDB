use crate::constants::{CHUNK_FOOTER_SIZE, CHUNK_HEADER_SIZE, CHUNK_SIZE};
use crate::index::{Lsm, LsmSettings};
use crate::storage::{FileCategory, FileId, Storage};
use crate::wal::chunk::{Chunk, ChunkInfo};
use crate::wal::footer::ChunkFooter;
use crate::wal::header::ChunkHeader;
use bytes::{Buf, Bytes, BytesMut};
use geth_common::{ExpectedRevision, Propose, WriteResult};
use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, RwLock};

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

#[derive(Clone)]
pub struct ChunkManager<S> {
    buffer: BytesMut,
    index: Lsm<S>,
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

        let index = Lsm::load(LsmSettings::default(), storage.clone())?;

        Ok(Self {
            index,
            buffer: BytesMut::new(),
            storage,
            state: Arc::new(RwLock::new(State { chunks, writer })),
        })
    }

    pub fn append(
        &self,
        stream_name: String,
        _expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> io::Result<WriteResult> {
        let mut state = self.state.write().unwrap();
        for (offset, propose) in events.into_iter().enumerate() {}
        todo!()
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
