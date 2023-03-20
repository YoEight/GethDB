use crate::backend::esdb::types::{
    Checkpoint, Chunk, ChunkFooter, ChunkInfo, FooterFlags, PrepareLog, CHUNK_FILE_SIZE,
    CHUNK_FOOTER_SIZE, CHUNK_HEADER_SIZE, CHUNK_SIZE,
};
use crate::backend::esdb::utils::md5_hash_chunk_file;
use bytes::BytesMut;
use std::collections::HashMap;
use std::fs::{read_dir, OpenOptions};
use std::io;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tracing::debug;

pub struct ChunkManager {
    root_fs: PathBuf,
    current_chunk_idx: usize,
    chunks: Vec<Chunk>,
    writer: Checkpoint,
    epoch: Checkpoint,
    chunk_count: usize,
    buffer: BytesMut,
}

impl ChunkManager {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut root_fs = PathBuf::new();
        let mut chunk_count = 0usize;
        let mut current_chunk_idx = 0usize;

        // Configuration
        root_fs.push(path);
        std::fs::create_dir_all(root_fs.as_path())?;

        // writer checkpoint.
        root_fs.push("writer.chk");
        let mut writer = Checkpoint::new(root_fs.as_path())?;
        root_fs.pop();

        // epoch checkpoint.
        root_fs.push("epoch.chk");
        let mut epoch = Checkpoint::new(root_fs.as_path())?;
        root_fs.pop();

        let chunk_files = list_chunk_files(root_fs.as_path())?;
        let mut chunks = Vec::new();

        if chunk_files.is_empty() {
            chunks.push(Chunk::new(root_fs.clone(), 0)?);
            chunk_count = 1;
        } else {
            let last_chunk_idx = chunk_files.len() - 1;
            let log_position = writer.read()?;
            for (idx, chunk_file_name) in chunk_files.into_iter().enumerate() {
                root_fs.push(chunk_file_name);
                let mut chunk = Chunk::load(root_fs.as_path())?;
                let chunk_start_number = chunk.header.chunk_start_number;
                let chunk_end_number = chunk.header.chunk_end_number;

                chunk_count = chunk.header.chunk_end_number as usize + 1;
                root_fs.pop();

                if idx == last_chunk_idx {
                    // TODO - handle the case where it's a complete chunk.
                    // TODO - handle the case where it's full ongoing chunk.
                    // TODO - In both cases, we would need to create a new chunk on the spot.

                    let local_log_position =
                        log_position as u64 - chunk_start_number as u64 * CHUNK_SIZE as u64;
                    let raw_position = CHUNK_HEADER_SIZE as u64 + local_log_position;

                    // Because that chunk if the ongoing chunk we move its offset next to
                    // available space.
                    chunk.file.seek(SeekFrom::Start(raw_position))?;
                    current_chunk_idx = idx;
                }

                chunks.push(chunk);
            }
        }

        Ok(Self {
            writer,
            epoch,
            root_fs,
            current_chunk_idx: 0,
            chunks: vec![],
            chunk_count,
            buffer: BytesMut::new(),
        })
    }

    pub fn chunk_from_logical_position(&mut self, log_position: i64) -> Option<&mut Chunk> {
        let mut result = None;

        for chunk in self.chunks.iter_mut() {
            if chunk.header.chunk_start_number as usize * CHUNK_SIZE > log_position as usize {
                break;
            }

            result = Some(chunk);
        }

        result
    }

    fn current_chunk(&mut self) -> &mut Chunk {
        self.chunks
            .get_mut(self.current_chunk_idx)
            .expect("to be defined")
    }

    pub fn log_position(&mut self) -> io::Result<i64> {
        self.writer.read()
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.current_chunk().file.flush()?;
        self.writer.flush()?;

        Ok(())
    }

    pub fn write_prepare_record(&mut self, prepare: PrepareLog) -> io::Result<i64> {
        let log_position = self.writer.read()?;
        prepare.write(1u8, log_position, &mut self.buffer);
        let content = self.buffer.split().freeze();

        {
            let current = self.current_chunk();
            let raw_position = current.header.raw_position(log_position as u64) as usize;

            debug!("Starting switching to newer chunk...");
            debug!("Computed physical_data_size: {}", raw_position);

            // It means we need to create a new chunk because there no space in the current one.
            // 8 stands for the 4bits we add at the beginning and end of the record when writing to a file.
            if raw_position + content.len() + 8 > CHUNK_HEADER_SIZE + CHUNK_SIZE {
                let physical_data_size = raw_position as i32 - CHUNK_HEADER_SIZE as i32;
                let mut footer = ChunkFooter {
                    flags: FooterFlags::IS_COMPLETED | FooterFlags::IS_MAP_12_BYTES,
                    is_completed: true,
                    is_map_12bytes: true,
                    physical_data_size,
                    logical_data_size: physical_data_size as i64,
                    map_size: 0,
                    hash: [0u8; 16],
                };

                footer.write(&mut self.buffer);
                current
                    .file
                    .seek(SeekFrom::End(-(CHUNK_FOOTER_SIZE as i64)))?;
                current
                    .file
                    .write_all(self.buffer.split().freeze().as_ref())?;

                debug!("Start computing MD5 hash of '{:?}'...", current.file);
                let hash = md5_hash_chunk_file(&current, &mut current.file)?;
                current.file.seek(SeekFrom::End(-16))?;
                current.file.write_all(&hash)?;
                footer.hash = hash;
                debug!("Complete");

                let new_chunk = Chunk::new(self.root_fs.clone(), self.chunk_count)?;
                self.chunk_count += 1;
                self.writer.write(new_chunk.start_position() as i64)?;
                self.current_chunk_idx += 1;
                self.chunks.push(new_chunk);
            }
        }

        let log_position = self.current_chunk().write(content)?;

        self.writer.write(log_position)?;

        Ok(log_position)
    }
}

fn chunk_filename_from(seq_number: usize, version: usize) -> String {
    format!("chunk-{:06}.{:06}", seq_number, version)
}

fn list_chunk_files(root: impl AsRef<Path>) -> io::Result<Vec<String>> {
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

    let mut files = latest_versions
        .values()
        .map(|info| chunk_filename_from(info.seq_num, info.version))
        .collect::<Vec<String>>();

    files.sort();

    Ok(files)
}
