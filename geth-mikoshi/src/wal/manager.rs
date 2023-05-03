use crate::storage::{FileCategory, Storage};
use crate::wal::chunk::{Chunk, ChunkInfo};
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
    current_chunk: usize,
    writer: u64,
}

#[derive(Clone)]
pub struct ChunkManager<S> {
    storage: S,
    state: Arc<RwLock<State>>,
}

impl<S> ChunkManager<S>
where
    S: Storage,
{
    pub fn load(storage: S) -> io::Result<Self> {
        let mut chunks = BTreeMap::<usize, ChunkInfo>::new();

        for info in storage.list(Chunks)? {
            if let Some(chunk) = chunks.get_mut(&info.seq_num) {
                if chunk.version < info.version {
                    *chunk = info;
                }
            } else {
                chunks.insert(info.seq_num, info);
            }
        }

        for info in chunks.into_values() {}

        todo!()
    }
}
