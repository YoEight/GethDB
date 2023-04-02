use crate::backend::esdb::types::{
    ChunkBis, ChunkFooter, ChunkHeader, ChunkInfo, RecordHeader, CHUNK_FILE_SIZE,
    CHUNK_FOOTER_SIZE, CHUNK_HEADER_SIZE,
};
use crate::backend::esdb::utils::chunk_filename_from;
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::fs::{read_dir, File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub fn list_chunk_files(root: impl AsRef<Path>) -> io::Result<Vec<ChunkInfo>> {
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

pub fn create_new_chunk_file(root: &PathBuf, chunk: &ChunkBis) -> io::Result<File> {
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

pub fn load_chunk(
    buffer: &mut BytesMut,
    root: &PathBuf,
    info: ChunkInfo,
) -> eyre::Result<ChunkBis> {
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

// TODO - Consider opening file in read or write only mode.
pub fn open_chunk_file(root: &PathBuf, chunk: &ChunkBis, log_position: u64) -> io::Result<File> {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open(root.join(chunk.filename()))?;

    file.seek(SeekFrom::Start(chunk.raw_position(log_position)))?;

    Ok(file)
}

pub fn read_chunk_record(
    chunk_file: &mut File,
    buffer: &mut BytesMut,
) -> io::Result<(RecordHeader, Bytes)> {
    let pre_size = chunk_file.read_i32::<LittleEndian>()?;
    buffer.reserve(pre_size as usize);

    unsafe {
        buffer.set_len(pre_size as usize);
    }

    chunk_file.read_exact(&mut buffer.as_mut()[..pre_size as usize])?;
    let mut content = buffer.split().freeze();
    let post_size = chunk_file.read_i32::<LittleEndian>()?;

    if pre_size != post_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Corrupt chunk {} != {} ({}): file {:?}",
                pre_size,
                post_size,
                content.len(),
                chunk_file,
            ),
        ));
    }

    Ok((RecordHeader::new(pre_size as usize, &mut content)?, content))
}
