use crate::backend::esdb::manager::ChunkManager;
use crate::backend::esdb::types::{
    Checkpoint, Chunk, ChunkFooter, ChunkHeader, FooterFlags, PrepareFlags, PrepareLog,
    ProposedEvent, CHUNK_FILE_SIZE, CHUNK_FOOTER_SIZE, CHUNK_HEADER_SIZE, CHUNK_SIZE,
};
use crate::backend::esdb::utils::{chunk_filename_from, list_chunk_files, md5_hash_chunk_file};
use crate::backend::Backend;
use crate::MikoshiStream;
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

mod manager;
pub mod parsing;
pub mod types;
mod utils;

pub struct EsdbBackend {
    revisions: HashMap<String, u64>,
    #[allow(dead_code)]
    chunk_count: usize,
    #[allow(dead_code)]
    db_root: PathBuf,
    chunks: HashMap<usize, Chunk>,
    writer_chk: Checkpoint,
    epoch_chk: Checkpoint,
    ongoing_chunk_file: File,
    buffer: BytesMut,
}

impl EsdbBackend {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut db_root = PathBuf::new();
        let mut chunk_count = 0usize;
        let mut chunks = HashMap::<usize, Chunk>::new();

        db_root.push(path);
        // Configuration
        std::fs::create_dir_all(db_root.as_path())?;

        // writer checkpoint.
        db_root.push("writer.chk");
        let mut writer_chk = Checkpoint::new(db_root.as_path())?;
        db_root.pop();

        // epoch checkpoint.
        db_root.push("epoch.chk");
        let mut epoch_chk = Checkpoint::new(db_root.as_path())?;
        db_root.pop();

        let mut all_chunks = list_chunk_files(db_root.as_path())?;

        // Means we are dealing with an empty database.
        let ongoing_chunk_file = if all_chunks.is_empty() {
            let filename = chunk_filename_from(0, 0);
            db_root.push(filename);

            let mut file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(db_root.as_path())?;

            file.set_len(CHUNK_FILE_SIZE as u64)?;
            let chunk = Chunk::new(0);
            chunk.header.write(&mut file)?;
            file.flush()?;

            chunks.insert(0, chunk);
            chunk_count = 1;
            db_root.pop();

            file
        } else {
            let log_position = writer_chk.read()?;
            let last_chunk_idx = all_chunks.len() - 1;
            let mut chunk_idx = 0usize;

            loop {
                let chunk_filename = all_chunks.remove(0);
                db_root.push(chunk_filename.as_str());

                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(db_root.as_path())?;

                let mut file = BufReader::new(file);
                let header = ChunkHeader::read(&mut file)?;
                let footer = ChunkFooter::read(&mut file)?;
                let chunk = Chunk { header, footer };

                let chunk_start_number = chunk.header.chunk_start_number as i64;
                let chunk_end_number = chunk.header.chunk_end_number as usize;
                chunks.insert(chunk_count, chunk);
                chunk_count = chunk_end_number as usize + 1;
                db_root.pop();

                if chunk_idx == last_chunk_idx {
                    // TODO - handle the case where it's a complete chunk.
                    // TODO - handle the case where it's full ongoing chunk.
                    // In both cases, we would need to create a new chunk on the spot.
                    let mut file = file.into_inner();
                    let local_log_position = log_position - chunk_start_number * CHUNK_SIZE as i64;
                    let raw_position = CHUNK_HEADER_SIZE as u64 + local_log_position as u64;

                    file.seek(SeekFrom::Start(raw_position))?;

                    break file;
                }

                chunk_idx += 1;
            }
        };

        Ok(Self {
            revisions: Default::default(),
            chunk_count,
            db_root,
            chunks,
            writer_chk,
            epoch_chk,
            ongoing_chunk_file,
            buffer: BytesMut::new(),
        })
    }

    fn switch_to_new_chunk(&mut self, log_position: u64) -> io::Result<()> {
        let mut current_chunk = self.current_chunk()?;
        let current_file_position = current_chunk.header.raw_position(log_position);
        let physical_data_size = current_file_position - CHUNK_HEADER_SIZE as u64;
        debug!("computed physical_data_size: {}", physical_data_size);
        let footer = ChunkFooter {
            flags: FooterFlags::IS_COMPLETED | FooterFlags::IS_MAP_12_BYTES,
            is_completed: true,
            is_map_12bytes: true,
            physical_data_size: physical_data_size as i32,
            logical_data_size: physical_data_size as i64,
            map_size: 0,
            hash: [0u8; 16],
        };

        current_chunk.footer = Some(footer);

        footer.write(&mut self.buffer);
        self.ongoing_chunk_file
            .seek(SeekFrom::End(-(CHUNK_FOOTER_SIZE as i64)))?;
        self.ongoing_chunk_file
            .write_all(self.buffer.split().freeze().as_ref())?;

        debug!(
            "Start computing MD5 hash of '{:?}'...",
            self.ongoing_chunk_file
        );
        let hash = md5_hash_chunk_file(&current_chunk, &mut self.ongoing_chunk_file)?;
        debug!("Completed");
        self.ongoing_chunk_file.seek(SeekFrom::End(-16))?;

        // We write the MD5 hash at the end of the footer.
        self.ongoing_chunk_file.write_all(&hash)?;
        debug!("Complete writing down the MD5 hash value");
        current_chunk.footer.as_mut().unwrap().hash = hash;

        let mut filepath = self.db_root.clone();
        let chunk_name = chunk_filename_from(self.chunk_count, 0);

        filepath.push(chunk_name);

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(filepath.as_path())?;

        file.set_len(CHUNK_FILE_SIZE as u64)?;
        let new_chunk = Chunk::new(self.chunk_count);
        new_chunk.header.write(&mut file)?;
        file.flush()?;

        // TODO - We force update the current value of the current log position. We might
        // consider if we shouldn't do this in a different location in case it's conflicting with
        // other administrative operations.
        // Context: We currently only use that function while calling `append_log_records` function.
        self.writer_chk.write(new_chunk.start_position() as i64)?;
        self.chunks.insert(self.chunk_count, new_chunk);
        self.chunk_count += 1;
        self.ongoing_chunk_file = file;

        Ok(())
    }

    fn current_chunk(&mut self) -> io::Result<Chunk> {
        Ok(*self.current_chunk_mut()?)
    }

    fn current_chunk_mut(&mut self) -> io::Result<&mut Chunk> {
        let log_position = self.writer_chk.read()?;
        let chunk_num = log_position as usize / CHUNK_SIZE;

        Ok(self.chunks.get_mut(&chunk_num).expect("to be defined"))
    }
}

impl Backend for EsdbBackend {
    fn append(
        &mut self,
        stream_name: String,
        _expected: ExpectedRevision,
        events: Vec<Propose>,
    ) -> io::Result<WriteResult> {
        let mut revision = self
            .revisions
            .get(stream_name.as_str())
            .copied()
            .unwrap_or_default();

        let mut log_position = self.writer_chk.read()?;
        debug!("Current log position: {}", log_position);
        let mut current_chunk = self.current_chunk()?;
        let mut buffer = bytes::BytesMut::new();
        let mut physical_data_size = if let Some(footer) = current_chunk.footer.as_ref() {
            footer.physical_data_size
        } else {
            0
        };

        for propose in events {
            let correlation_id = Uuid::new_v4();
            let mut flags: PrepareFlags = PrepareFlags::HAS_DATA
                | PrepareFlags::TRANSACTION_START
                | PrepareFlags::TRANSACTION_END
                | PrepareFlags::IS_COMMITTED
                | PrepareFlags::IS_JSON;

            /*if event.is_json {
                flags |= PrepareFlags::IS_JSON;
            }*/

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

            prepare.write(1u8, log_position, &mut buffer);

            let raw_position = current_chunk.header.raw_position(log_position as u64) as usize;

            // It means we need to create a new chunk because there no space in the current chunk.
            if raw_position + buffer.len() + 8 > CHUNK_HEADER_SIZE + CHUNK_SIZE {
                debug!("Starting switching to newer chunk...");
                debug!("Current physical_data_size: {}", physical_data_size);
                self.switch_to_new_chunk(log_position as u64)?;
                debug!("Completed!");
                current_chunk = self.current_chunk()?;
            }

            let content = buffer.split().freeze();

            self.ongoing_chunk_file
                .write_i32::<LittleEndian>(content.len() as i32)?;
            self.ongoing_chunk_file.write_all(content.as_ref())?;
            self.ongoing_chunk_file
                .write_i32::<LittleEndian>(content.len() as i32)?;
            self.ongoing_chunk_file.flush()?;

            let current_position = self.ongoing_chunk_file.stream_position()?;
            physical_data_size = current_position as i32 - CHUNK_HEADER_SIZE as i32;
            log_position = current_chunk.logical_position(physical_data_size as u64) as i64;
            revision += 1;
        }

        self.revisions.insert(stream_name, revision);
        self.writer_chk.write(log_position)?;
        self.writer_chk.flush()?;

        Ok(WriteResult {
            next_expected_version: ExpectedRevision::Revision(revision),
            position: Position(log_position as u64),
        })
    }

    fn read(
        &self,
        stream_name: String,
        starting: Revision<u64>,
        direction: Direction,
    ) -> MikoshiStream {
        todo!()
    }
}
