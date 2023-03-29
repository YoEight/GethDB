use crate::backend::esdb::types::{
    Checkpoint, Chunk, ChunkFooter, ChunkInfo, EpochRecord, FooterFlags, PrepareLog, RecordType,
    SystemLog, SystemRecordFormat, SystemRecordType, CHUNK_FILE_SIZE, CHUNK_FOOTER_SIZE,
    CHUNK_HEADER_SIZE, CHUNK_SIZE,
};
use crate::backend::esdb::utils::md5_hash_chunk_file;
use crate::Entry;
use bytes::{BufMut, BytesMut};
use chrono::{DateTime, TimeZone, Utc};
use geth_common::Position;
use std::collections::HashMap;
use std::fs::{read_dir, File, OpenOptions};
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
        let root_fs = PathBuf::from(path.as_ref());
        let mut chunk_count = 0usize;
        let mut current_chunk_idx = 0usize;

        // Configuration
        std::fs::create_dir_all(root_fs.as_path())?;

        // writer checkpoint.
        let mut writer = Checkpoint::new(root_fs.join("writer.chk"))?;
        // epoch checkpoint.
        let mut epoch = Checkpoint::new(root_fs.join("epoch.chk"))?;

        let chunk_files = list_chunk_files(root_fs.as_path())?;
        let mut chunks = Vec::new();

        if chunk_files.is_empty() {
            chunks.push(Chunk::new(root_fs.clone(), 0)?);
            chunk_count = 1;
        } else {
            let log_position = writer.read()?;
            for (idx, chunk_file_name) in chunk_files.into_iter().enumerate() {
                let mut chunk = Chunk::load(root_fs.join(chunk_file_name))?;

                chunk_count = chunk.header.chunk_end_number as usize + 1;

                // Means that we are dealing with the ongoing chunk.
                if chunk.contains_log_position(log_position as u64) {
                    // TODO - handle the case where it's a complete chunk.
                    // TODO - handle the case where it's full ongoing chunk.
                    // TODO - In both cases, we would need to create a new chunk on the spot.

                    let raw_position = chunk.header.raw_position(log_position as u64);

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
            current_chunk_idx,
            chunks,
            chunk_count,
            buffer: BytesMut::new(),
        })
    }

    pub fn root(&self) -> PathBuf {
        self.root_fs.clone()
    }

    pub fn chunks(&self) -> &Vec<Chunk> {
        &self.chunks
    }

    pub fn writer_checkpoint(&mut self) -> io::Result<i64> {
        self.writer.read()
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

    fn current_chunk_local_raw_position(&mut self, log_position: i64) -> u64 {
        self.current_chunk()
            .header
            .raw_position(log_position as u64) as u64
    }

    pub fn log_position(&mut self) -> io::Result<i64> {
        self.writer.read()
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.current_chunk().file.flush()?;
        self.writer.flush()?;

        Ok(())
    }

    fn prepare_write(&mut self, log_position: i64, byte_count: usize) -> io::Result<()> {
        let raw_position = self.current_chunk_local_raw_position(log_position) as usize;

        if raw_position + byte_count + 8 > CHUNK_HEADER_SIZE + CHUNK_SIZE {
            debug!("Starting switching to newer chunk...");

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

            let content = self.buffer.split().freeze();
            let current = self.current_chunk();

            current
                .file
                .seek(SeekFrom::End(-(CHUNK_FOOTER_SIZE as i64)))?;
            current.file.write_all(content.as_ref())?;

            debug!("Start computing MD5 hash of '{:?}'...", current.file);
            let hash = current.md5_hash()?;
            current.file.seek(SeekFrom::End(-16))?;
            current.file.write_all(&hash)?;
            footer.hash = hash;
            debug!("Complete");

            let new_chunk = Chunk::new(self.root_fs.clone(), self.chunk_count)?;
            self.chunk_count += 1;
            self.writer.write(new_chunk.start_position() as i64)?;
            self.current_chunk_idx += 1;
            self.chunks.push(new_chunk);

            debug!("Chunk switching completed");
        }

        Ok(())
    }

    pub fn write_prepare_record(&mut self, prepare: PrepareLog) -> io::Result<i64> {
        let log_position = self.writer.read()?;
        prepare.write(1u8, log_position, &mut self.buffer);
        let content = self.buffer.split().freeze();

        self.prepare_write(log_position, content.len())?;

        let log_position = self.current_chunk().write(content)?;

        self.writer.write(log_position)?;

        Ok(log_position)
    }

    pub fn write_epoch_record(&mut self, record: EpochRecord) -> io::Result<i64> {
        let log_position = self.writer.read()?;
        let mut writer = self.buffer.clone().writer();
        serde_json::to_writer(&mut writer, &record)?;

        let record = SystemLog {
            timestamp: record.time.timestamp(),
            kind: SystemRecordType::Epoch,
            format: SystemRecordFormat::Json,
            data: writer.into_inner().freeze(),
        };

        record.write(&mut self.buffer);
        let content = self.buffer.clone().freeze();

        self.prepare_write(log_position, content.len())?;
        let log_position = self.current_chunk().write(content)?;
        self.writer.write(log_position)?;
        self.flush()?;

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

pub struct FullScan {
    buffer: BytesMut,
    end_log_position: u64,
    log_position: u64,
    root: PathBuf,
    chunks: Vec<String>,
    chunk: Option<Chunk>,
}

impl FullScan {
    pub fn new(
        buffer: BytesMut,
        root: PathBuf,
        chunks: Vec<String>,
        end_log_position: u64,
    ) -> Self {
        Self {
            buffer,
            end_log_position,
            log_position: 0,
            root,
            chunks,
            chunk: None,
        }
    }

    pub fn next(&mut self) -> io::Result<Option<Entry>> {
        loop {
            if self.log_position == self.end_log_position {
                return Ok(None);
            }

            // We check that the data we are looking for is still in the current chunk. Otherwise,
            // we move to the next one.
            if let Some(mut chunk) = self.chunk.take() {
                if self.log_position < chunk.end_position() {
                    self.chunk = Some(chunk);
                }
            }

            // If no current chunk is allocated, we create a new one.
            if self.chunk.is_none() {
                let filename = self.chunks.remove(0);
                let mut chunk = Chunk::load(self.root.join(filename))?;

                chunk.set_cursor_at(self.log_position)?;
                self.chunk = Some(chunk);
            }

            let chunk = self.chunk.as_mut().unwrap();
            let (header, record) = chunk.read_record(&mut self.buffer)?;
            let next_record_log_position = header.log_position + header.size as u64 + 8;

            self.log_position = next_record_log_position;

            if header.r#type != RecordType::Prepare {
                continue;
            }

            let prepare = PrepareLog::read(1u8, record)?;

            return Ok(Some(Entry {
                id: prepare.event_id,
                stream_name: prepare.event_stream_id,
                revision: prepare.expected_version as u64,
                data: prepare.data,
                position: Position(header.log_position),
                created: Utc.timestamp_millis_opt(prepare.timestamp).unwrap(),
            }));
        }
    }
}
