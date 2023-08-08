use crate::constants::{CHUNK_HEADER_SIZE, CHUNK_SIZE};
use crate::storage::FileId;
use crate::wal::chunks::footer::ChunkFooter;
use crate::wal::chunks::header::ChunkHeader;
use nom::bytes::complete::{tag, take_till1};
use nom::IResult;
use std::cmp::Ordering;
use uuid::Uuid;

#[derive(Debug, Copy, Clone)]
pub struct ChunkInfo {
    pub seq_num: usize,
    pub version: usize,
}

impl PartialEq<Self> for ChunkInfo {
    fn eq(&self, other: &Self) -> bool {
        self.seq_num.eq(&other.seq_num)
    }
}

impl PartialOrd<Self> for ChunkInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.seq_num.partial_cmp(&other.seq_num)
    }
}

impl Eq for ChunkInfo {}

impl Ord for ChunkInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.seq_num.cmp(&other.seq_num)
    }
}

impl ChunkInfo {
    pub fn file_id(&self) -> FileId {
        FileId::Chunk {
            num: self.seq_num,
            version: self.version,
        }
    }

    pub fn from_chunk_filename(input: &str) -> Option<Self> {
        if let Ok((_, info)) = Self::parse_chunk_filename(input) {
            return Some(info);
        }

        None
    }

    pub fn parse_chunk_filename(input: &str) -> IResult<&str, Self> {
        let (input, _) = tag("chunk-")(input)?;
        let (input, seq_str) = take_till1(|c: char| !c.is_ascii_digit())(input)?;

        let seq_num = match seq_str.parse::<usize>() {
            Ok(n) => n,
            Err(_) => {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    seq_str,
                    nom::error::ErrorKind::Fail,
                )))
            }
        };

        let (input, _) = tag(".")(input)?;
        let (input, ver_str) = take_till1(|c: char| !c.is_ascii_digit())(input)?;

        let version = match ver_str.parse::<usize>() {
            Ok(n) => n,
            Err(_) => {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    ver_str,
                    nom::error::ErrorKind::Fail,
                )))
            }
        };

        let info = ChunkInfo { seq_num, version };

        Ok((input, info))
    }
}
#[derive(Clone, Debug)]
pub struct Chunk {
    pub info: ChunkInfo,
    pub header: ChunkHeader,
    pub footer: Option<ChunkFooter>,
}

impl Chunk {
    pub fn new(num: usize) -> Self {
        Self {
            info: ChunkInfo {
                seq_num: num,
                version: 0,
            },
            header: ChunkHeader {
                version: 0,
                chunk_size: CHUNK_SIZE,
                chunk_start_number: num,
                chunk_end_number: num,
                chunk_id: Uuid::new_v4(),
            },
            footer: None,
        }
    }

    pub fn next_chunk(&self) -> Self {
        Self::new(self.info.seq_num + 1)
    }

    pub fn file_id(&self) -> FileId {
        self.info.file_id()
    }

    pub fn local_physical_position(&self, log_position: u64) -> u64 {
        log_position - self.start_position()
    }

    pub fn raw_position(&self, log_position: u64) -> u64 {
        CHUNK_HEADER_SIZE as u64 + self.local_physical_position(log_position)
    }

    pub fn start_position(&self) -> u64 {
        self.header.chunk_start_number as u64 * CHUNK_SIZE as u64
    }

    pub fn end_position(&self) -> u64 {
        (self.header.chunk_end_number as u64 + 1) * CHUNK_SIZE as u64
    }

    pub fn contains_log_position(&self, log_position: u64) -> bool {
        log_position >= self.start_position() && log_position < self.end_position()
    }

    pub fn remaining_space_from(&self, log_position: u64) -> u64 {
        if self.end_position() <= log_position {
            0
        } else {
            self.end_position() - log_position
        }
    }
}
