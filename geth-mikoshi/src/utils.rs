use crate::types::{Chunk, ChunkInfo, CHUNK_HEADER_SIZE};
use md5::Digest;
use std::cmp::min;
use std::collections::HashMap;
use std::fs::{read_dir, File};
use std::io;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use tracing::debug;

pub fn list_chunk_files(root: impl AsRef<Path>) -> io::Result<Vec<String>> {
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

pub fn chunk_filename_from(seq_number: usize, version: usize) -> String {
    format!("chunk-{:06}.{:06}", seq_number, version)
}

pub fn md5_hash_chunk_file(chunk: &Chunk, file: &mut File) -> io::Result<[u8; 16]> {
    #[derive(Debug)]
    enum State {
        HeaderAndRecords,
        Footer,
    }

    let mut state = State::HeaderAndRecords;
    let mut buffer = [0u8; 4_096];
    let mut digest = md5::Md5::default();
    let footer = chunk.footer.expect("to be defined");

    file.seek(SeekFrom::Start(0))?;

    loop {
        debug!("Current MD5 state: {:?}", state);
        let mut to_read = match state {
            State::HeaderAndRecords => CHUNK_HEADER_SIZE + footer.physical_data_size as usize,
            State::Footer => {
                let meta = file.metadata()?;
                meta.len() as usize - CHUNK_HEADER_SIZE - footer.physical_data_size as usize - 16
            }
        };

        debug!("Need to read: {}...", to_read);
        while to_read > 0 {
            let len = min(to_read, buffer.len());

            file.read_exact(&mut buffer[..len])?;
            digest.update(&buffer[..len]);
            to_read -= len;
        }
        debug!("Completed");

        match state {
            State::HeaderAndRecords => state = State::Footer,
            State::Footer => {
                let mut hash = [0u8; 16];
                // We might do unnecessary copy here. Considering that we only have to deal with
                // 16 bytes array, it's ok.
                hash.copy_from_slice(digest.finalize().as_slice());

                return Ok(hash);
            }
        };
    }
}

pub fn variable_string_length_bytes_size(value: usize) -> usize {
    let mut value = value as u64;
    let mut count = 0usize;

    while value > 0x7F {
        count += 1;
        value >>= 7;
    }

    count + 1
}
