use crate::backend::esdb::types::{
    Checkpoint, ChunkBis, ChunkFooter, ChunkHeader, ChunkInfo, ChunkManagerBis, FileType,
    FooterFlags, PrepareFlags, PrepareLog, CHUNK_FILE_SIZE, CHUNK_FOOTER_SIZE, CHUNK_HEADER_SIZE,
    CHUNK_SIZE,
};
use crate::backend::esdb::utils::{chunk_filename_from, list_chunk_files, md5_hash_chunk_file};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::Utc;
use geth_common::{ExpectedRevision, Propose, WriteResult};
use std::collections::HashMap;
use std::fs::{read_dir, File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Index;
use std::path::{Path, PathBuf};
use tracing::debug;
use uuid::Uuid;

pub struct BlockingBackend {
    root: PathBuf,
    manager: ChunkManagerBis,
    writer: Checkpoint,
    epoch: Checkpoint,
    buffer: BytesMut,
}

impl BlockingBackend {
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

        let chunk_info = list_chunks(root.as_path())?;
        let mut chunks = Vec::new();

        if chunk_info.is_empty() {
            let new_chunk = ChunkBis::new(0);
            create_new_chunk_file(&root, &new_chunk)?;
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

fn create_new_chunk_file(root: &PathBuf, chunk: &ChunkBis) -> io::Result<File> {
    let path = root.join(chunk.filename());

    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path.as_path())?;

    file.set_len(CHUNK_FILE_SIZE as u64)?;
    chunk.header.write(&mut file)?;

    file.flush()?;

    Ok(file)
}

fn load_chunk(buffer: &mut BytesMut, root: &PathBuf, info: ChunkInfo) -> eyre::Result<ChunkBis> {
    buffer.reserve(CHUNK_HEADER_SIZE);

    unsafe {
        buffer.set_len(CHUNK_HEADER_SIZE);
    }

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

fn open_chunk_file(root: &PathBuf, chunk: &ChunkBis, log_position: u64) -> io::Result<File> {
    let mut file = OpenOptions::new()
        .write(true)
        .open(root.join(chunk.filename()))?;

    file.seek(SeekFrom::Start(chunk.raw_position(log_position)))?;

    Ok(file)
}

fn append_events(
    root: &PathBuf,
    buffer: &mut BytesMut,
    manager: &mut ChunkManagerBis,
    stream_name: String,
    expected: ExpectedRevision,
    events: Vec<Propose>,
) -> eyre::Result<WriteResult> {
    let mut ongoing_chunk = manager.ongoing_chunk();
    let mut ongoing_chunk_file = open_chunk_file(root, &ongoing_chunk, manager.writer)?;
    let mut write_plan = WritePlan::new(manager.writer);
    let transaction_position = manager.writer as i64;

    // TODO - Implement proper stream revision tracking;
    let mut revision = 0u64;

    for (offset, propose) in events.into_iter().enumerate() {
        let correlation_id = Uuid::new_v4();
        let record_position = manager.writer;

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
            data: bytes::Bytes::from(propose.data.to_vec()),
            metadata: bytes::Bytes::from(Vec::<u8>::default()),
        };

        // Record Header stuff
        buffer.put_u8(0);
        buffer.put_u8(1);
        buffer.put_u64_le(record_position);
        prepare.write(1u8, buffer);

        let record = buffer.split().freeze();
        let size = record.len();

        buffer.put_i32_le(size as i32);
        buffer.put(record);
        buffer.put_i32_le(size as i32);

        let record = buffer.split().freeze();

        manager.writer += record.len() as u64;

        loop {
            if ongoing_chunk.contains_log_position(manager.writer) {
                ongoing_chunk_file.write_all(record.as_ref())?;
                break;
            } else {
                let new_chunk = ChunkBis::new(ongoing_chunk.num + 1);
                let new_file = create_new_chunk_file(root, &new_chunk)?;
                let footer = ongoing_chunk.complete(buffer, record_position);
                let content = buffer.split().freeze();

                ongoing_chunk_file.seek(SeekFrom::End(-(CHUNK_FOOTER_SIZE as i64)))?;
                ongoing_chunk_file.write_all(content.as_ref())?;

                debug!("Start computing MD5 hash of '{:?}'...", ongoing_chunk_file);
                ongoing_chunk.update_hash(md5_hash_chunk_file(&mut ongoing_chunk_file, footer)?);
                debug!("Complete");

                manager.update_chunk(ongoing_chunk);
                manager.chunks.push(new_chunk);
                ongoing_chunk = new_chunk;
                ongoing_chunk_file = new_file;
            }
        }

        revision += 1;
    }

    // TODO - Persist the writer checkpoint;
    todo!()
}

struct WritePlan {
    transaction_position: u64,
    ongoing_chunk: Vec<Bytes>,
    new_chunks: Vec<Bytes>,
}

impl WritePlan {
    fn new(transaction_position: u64) -> Self {
        Self {
            transaction_position,
            ongoing_chunk: vec![],
            new_chunks: vec![],
        }
    }
}
