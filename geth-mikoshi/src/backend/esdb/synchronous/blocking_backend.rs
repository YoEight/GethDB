use crate::backend::esdb::types::{
    Checkpoint, ChunkBis, ChunkFooter, ChunkHeader, ChunkInfo, ChunkManagerBis, FileType,
    CHUNK_FILE_SIZE, CHUNK_FOOTER_SIZE, CHUNK_HEADER_SIZE, CHUNK_SIZE,
};
use crate::backend::esdb::utils::{chunk_filename_from, list_chunk_files};
use std::collections::HashMap;
use std::fs::{read_dir, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub struct BlockingBackend {
    root: PathBuf,
    manager: ChunkManagerBis,
    writer: Checkpoint,
    epoch: Checkpoint,
    buffer: Vec<u8>,
}

impl BlockingBackend {
    pub fn new(path: impl AsRef<Path>) -> eyre::Result<Self> {
        let root = PathBuf::from(path.as_ref());
        let mut buffer = Vec::new();
        let mut chunk_count = 0usize;

        // Configuration
        std::fs::create_dir_all(root.as_path())?;

        // writer checkpoint.
        let mut writer = Checkpoint::new(root.join("writer.chk"))?;
        // epoch checkpoint.
        let mut epoch = Checkpoint::new(root.join("epoch.chk"))?;

        let chunk_info = list_chunks(root.as_path())?;
        let mut chunks = Vec::new();

        if chunk_info.is_empty() {
            let new_chunk = ChunkBis::new(0);
            create_new_chunk(&root, &new_chunk)?;
            chunks.push(new_chunk);
        } else {
            for info in chunk_info {
                let chunk = load_chunk(&mut buffer, &root, info)?;

                chunk_count = chunk.header.chunk_end_number as usize + 1;
                chunks.push(chunk);
            }
        }

        let manager = ChunkManagerBis {
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

fn list_chunks(root: impl AsRef<Path>) -> io::Result<Vec<ChunkInfo>> {
    let mut entries = read_dir(root)?;
    let mut latest_versions = HashMap::<usize, ChunkInfo>::new();

    while let Some(entry) = entries.next().transpose()? {
        if let Some(filename) = entry.file_name().to_str() {
            if let Ok((_, info)) = ChunkInfo::parse(filename) {
                if let Some(entry) = latest_versions.get_mut(&info.seq_num) {
                    if entry.version < info.version {
                        *entry = info;
                    }
                } else {
                    latest_versions.insert(info.seq_num, info);
                }
            }
        }
    }

    let mut files = latest_versions.into_values().collect::<Vec<ChunkInfo>>();

    files.sort();

    Ok(files)
}

fn create_new_chunk(root: &PathBuf, chunk: &ChunkBis) -> io::Result<()> {
    let path = root.join(chunk.filename());

    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path.as_path())?;

    file.set_len(CHUNK_FILE_SIZE as u64)?;
    chunk.header.write(&mut file)?;

    file.flush()
}

fn load_chunk(buffer: &mut Vec<u8>, root: &PathBuf, info: ChunkInfo) -> eyre::Result<ChunkBis> {
    buffer.reserve(CHUNK_HEADER_SIZE);
    let file = root.join(chunk_filename_from(info.seq_num, info.version));

    let mut file = OpenOptions::new().read(true).open(file.as_path())?;

    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut buffer[..CHUNK_HEADER_SIZE])?;

    let header = ChunkHeader::read_bis(&buffer[..CHUNK_HEADER_SIZE])?;

    file.seek(SeekFrom::End(-(CHUNK_FOOTER_SIZE as i64)))?;
    file.read_exact(&mut buffer[..CHUNK_FOOTER_SIZE])?;

    let footer = ChunkFooter::read_bis(&buffer[..CHUNK_FOOTER_SIZE])?;

    Ok(ChunkBis::from_info(info, header, footer))
}
