use crate::backend::esdb::parsing::{read_string, read_uuid, write_string};
use crate::backend::esdb::utils::{
    chunk_filename_from, md5_hash_chunk_file, variable_string_length_bytes_size,
};
use bitflags::bitflags;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::{buf, Buf, BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use geth_common::Record;
use nom::bytes::complete::{tag, take_till1};
use nom::{IResult, Or};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tracing::error;
use uuid::Uuid;

pub const CHUNK_SIZE: usize = 256 * 1024 * 1024;
pub const CHUNK_FOOTER_SIZE: usize = 128;
pub const CHUNK_HEADER_SIZE: usize = 128;
pub const CHUNK_FILE_SIZE: usize = aligned_size(CHUNK_SIZE + CHUNK_HEADER_SIZE + CHUNK_FOOTER_SIZE);

const fn aligned_size(size: usize) -> usize {
    if size % 4_096 == 0 {
        return size;
    }

    (size / 4_096 + 1) * 4_096
}

bitflags! {
    pub struct PrepareFlags: u16 {
        const NO_DATA = 0x00;
        const HAS_DATA = 0x01;
        const TRANSACTION_START = 0x02;
        const TRANSACTION_END = 0x04;
        const DELETED_STREAM = 0x08;
        const IS_COMMITTED = 0x20;
        const IS_JSON = 0x100;
    }
}

bitflags! {
    pub struct FooterFlags: u8 {
        const IS_COMPLETED = 0x1;
        const IS_MAP_12_BYTES = 0x2;
    }
}

pub struct Checkpoint {
    file: File,
    cached: Option<i64>,
}

impl Checkpoint {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let create_new = std::fs::metadata(path.as_ref()).is_err();
        let file = OpenOptions::new()
            .create_new(create_new)
            .read(true)
            .write(true)
            .open(path)?;

        let mut chk = Checkpoint { file, cached: None };

        if create_new {
            chk.write(0)?;
            chk.flush()?;
        }

        Ok(chk)
    }

    pub fn read(&mut self) -> io::Result<i64> {
        if let Some(value) = self.cached {
            return Ok(value);
        }

        self.file.seek(SeekFrom::Start(0))?;
        let value = self.file.read_i64::<LittleEndian>()?;
        self.cached = Some(value);

        Ok(value)
    }

    pub fn write(&mut self, pos: i64) -> io::Result<()> {
        self.cached = Some(pos);

        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if let Some(value) = self.cached {
            self.file.seek(SeekFrom::Start(0))?;
            self.file.write_i64::<LittleEndian>(value)?;
        }

        Ok(())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RecordType {
    Prepare,
    Commit,
    System,
}

#[derive(Debug, Copy, Clone)]
pub enum FileType {
    PTableFile,
    ChunkFile,
}

#[derive(Debug, Clone, Copy)]
pub struct ChunkHeader {
    pub file_type: FileType,
    pub version: u8,
    pub chunk_size: i32,
    pub chunk_start_number: i32,
    pub chunk_end_number: i32,
    pub is_scavenged: bool,
    pub chunk_id: Uuid,
}

impl ChunkHeader {
    pub fn field_get_chunk_id(&self) -> String {
        self.chunk_id.to_string()
    }

    /// Number of chunks located in the file.
    pub fn chunk_count(&self) -> usize {
        (self.chunk_start_number) as usize + 1
    }

    pub fn write<W>(&self, buf: &mut W) -> io::Result<()>
    where
        W: Write,
    {
        match self.file_type {
            FileType::ChunkFile => buf.write_u8(2),
            FileType::PTableFile => buf.write_u8(1),
        }?;

        buf.write_u8(self.version)?;
        buf.write_i32::<LittleEndian>(self.chunk_size)?;
        buf.write_i32::<LittleEndian>(self.chunk_start_number)?;
        buf.write_i32::<LittleEndian>(self.chunk_end_number)?;

        let is_scavenged = if self.is_scavenged { 1 } else { 0 };
        buf.write_i32::<LittleEndian>(is_scavenged)?;
        buf.write_all(self.chunk_id.as_bytes())?;
        let unused_space = [0u8; 94];
        buf.write_all(&unused_space)?;

        Ok(())
    }

    pub fn read_bis<R>(mut buf: R) -> eyre::Result<Self>
    where
        R: Buf,
    {
        let file_type = match buf.get_u8() {
            1 => FileType::PTableFile,
            2 => FileType::ChunkFile,
            x => eyre::bail!("Unknown chunk file type '{}'", x),
        };

        let version = buf.get_u8();
        let chunk_size = buf.get_i32_le();
        let chunk_start_number = buf.get_i32_le();
        let chunk_end_number = buf.get_i32_le();
        let is_scavenged = buf.get_i32_le() > 0;

        let mut chunk_id = [0u8; 16];
        buf.copy_to_slice(&mut chunk_id);
        // FIXME - I probably need to parse it as a windows UUID (namely GUID) instead of
        // a regular, standard UUID format.
        let chunk_id = match Uuid::from_slice(&chunk_id) {
            Ok(id) => id,
            Err(e) => eyre::bail!("Invalid chunk id format: {}", e),
        };

        Ok(Self {
            file_type,
            version,
            chunk_size,
            chunk_start_number,
            chunk_end_number,
            is_scavenged,
            chunk_id,
        })
    }

    pub fn read<R>(buf: &mut R) -> io::Result<Self>
    where
        R: Read + Seek,
    {
        buf.seek(SeekFrom::Start(0))?;

        let file_type = buf.read_u8()?;
        let file_type = match file_type {
            1 => FileType::PTableFile,
            2 => FileType::ChunkFile,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unknown chunk file type '{}'", file_type),
                ))
            }
        };

        let version = buf.read_u8()?;
        let chunk_size = buf.read_i32::<LittleEndian>()?;
        let chunk_start_number = buf.read_i32::<LittleEndian>()?;
        let chunk_end_number = buf.read_i32::<LittleEndian>()?;
        let is_scavenged = buf.read_i32::<LittleEndian>()? > 0;

        let mut chunk_id = [0u8; 16];
        buf.read_exact(&mut chunk_id)?;
        // FIXME - I probably need to parse it as a windows UUID (namely GUID) instead of
        // a regular, standard UUID format.
        let chunk_id = match Uuid::from_slice(&chunk_id) {
            Ok(id) => id,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid chunk id format: {}", e),
                ))
            }
        };

        Ok(ChunkHeader {
            file_type,
            version,
            chunk_size,
            chunk_start_number,
            chunk_end_number,
            is_scavenged,
            chunk_id,
        })
    }

    pub fn start_position(&self) -> u64 {
        self.chunk_start_number as u64 * CHUNK_SIZE as u64
    }

    pub fn local_physical_position(&self, log_position: u64) -> u64 {
        log_position - self.start_position()
    }

    pub fn raw_position(&self, log_position: u64) -> u64 {
        CHUNK_HEADER_SIZE as u64 + self.local_physical_position(log_position)
    }

    pub fn logical_position(&self, raw_position: u64) -> u64 {
        raw_position - CHUNK_HEADER_SIZE as u64 + self.start_position()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ChunkFooter {
    pub flags: FooterFlags,
    pub is_completed: bool,
    pub is_map_12bytes: bool,
    pub physical_data_size: i32,
    pub logical_data_size: i64,
    pub map_size: i32,
    pub hash: [u8; 16],
}

impl ChunkFooter {
    pub fn read_bis<R>(mut buf: R) -> eyre::Result<Option<Self>>
    where
        R: Buf,
    {
        let flags = FooterFlags::from_bits(buf.get_u8()).expect("valid footer flags");
        let is_completed = flags.contains(FooterFlags::IS_COMPLETED);

        if !is_completed {
            return Ok(None);
        }

        let is_map_12bytes = flags.contains(FooterFlags::IS_MAP_12_BYTES);
        let physical_data_size = buf.get_i32_le();
        let logical_data_size = if is_map_12bytes {
            buf.get_i64_le()
        } else {
            buf.get_i32_le() as i64
        };

        let map_size = buf.get_i32_le();

        if is_map_12bytes {
            buf.advance(92);
        } else {
            buf.advance(95);
        }

        let mut hash = [0u8; 16];
        buf.copy_to_slice(&mut hash);

        Ok(Some(ChunkFooter {
            flags,
            is_completed,
            is_map_12bytes,
            physical_data_size,
            logical_data_size,
            map_size,
            hash,
        }))
    }

    pub fn read<R>(buf: &mut R) -> io::Result<Option<Self>>
    where
        R: Read + Seek,
    {
        buf.seek(SeekFrom::End(-(CHUNK_FOOTER_SIZE as i64)))?;
        let flags = FooterFlags::from_bits(buf.read_u8()?).expect("valid footer flags");
        let is_completed = flags.contains(FooterFlags::IS_COMPLETED);

        if !is_completed {
            return Ok(None);
        }

        let is_map_12bytes = flags.contains(FooterFlags::IS_MAP_12_BYTES);
        let physical_data_size = buf.read_i32::<LittleEndian>()?;
        let logical_data_size = if is_map_12bytes {
            buf.read_i64::<LittleEndian>()?
        } else {
            let value = buf.read_i32::<LittleEndian>()?;
            value as i64
        };
        let map_size = buf.read_i32::<LittleEndian>()?;

        if is_map_12bytes {
            buf.seek(SeekFrom::Current(92))?;
        } else {
            buf.seek(SeekFrom::Current(95))?;
        }

        let mut hash = [0u8; 16];
        buf.read_exact(&mut hash)?;

        Ok(Some(ChunkFooter {
            flags,
            is_completed,
            is_map_12bytes,
            physical_data_size,
            logical_data_size,
            map_size,
            hash,
        }))
    }

    pub fn write<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        buf.put_u8(self.flags.bits);
        buf.put_i32_le(self.physical_data_size);

        if self.is_map_12bytes {
            buf.put_i32_le(self.logical_data_size as i32);
        } else {
            buf.put_i64_le(self.logical_data_size);
        }

        buf.put_i32_le(self.map_size);

        // Unused space.
        buf.put_slice(&[0u8; 92]);
        buf.put_slice(&self.hash);
    }
}

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
    pub fn parse(input: &str) -> IResult<&str, Self> {
        let (input, _) = tag("chunk-")(input)?;
        let (input, seq_str) = take_till1(|c: char| !c.is_ascii_digit())(input)?;

        let seq_num = match seq_str.parse::<usize>() {
            Ok(n) => n,
            Err(_) => {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    seq_str,
                    nom::error::ErrorKind::ParseTo,
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
                    nom::error::ErrorKind::ParseTo,
                )))
            }
        };

        let info = ChunkInfo { seq_num, version };

        Ok((input, info))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ChunkBis {
    pub num: usize,
    pub version: usize,
    pub header: ChunkHeader,
    pub footer: Option<ChunkFooter>,
}

impl ChunkBis {
    pub fn new(num: usize) -> Self {
        Self {
            num,
            version: 0,
            header: ChunkHeader {
                file_type: FileType::ChunkFile,
                version: 3,
                chunk_size: CHUNK_SIZE as i32,
                chunk_start_number: num as i32,
                chunk_end_number: num as i32,
                is_scavenged: false,
                chunk_id: Uuid::new_v4(),
            },
            footer: None,
        }
    }

    pub fn from_info(info: ChunkInfo, header: ChunkHeader, footer: Option<ChunkFooter>) -> Self {
        Self {
            num: info.seq_num,
            version: info.version,
            header,
            footer,
        }
    }

    pub fn filename(&self) -> String {
        chunk_filename_from(self.num, self.version)
    }

    pub fn local_physical_position(&self, log_position: u64) -> u64 {
        self.header.local_physical_position(log_position)
    }

    pub fn raw_position(&self, log_position: u64) -> u64 {
        self.header.raw_position(log_position)
    }

    pub fn start_position(&self) -> u64 {
        self.header.start_position()
    }

    pub fn end_position(&self) -> u64 {
        (self.header.chunk_end_number as u64 + 1) * CHUNK_SIZE as u64
    }

    pub fn contains_log_position(&self, log_position: u64) -> bool {
        log_position >= self.start_position() && log_position < self.end_position()
    }

    pub fn complete(&mut self, buffer: &mut BytesMut, log_position: u64) -> ChunkFooter {
        let physical_data_size = self.raw_position(log_position) as i32 - CHUNK_HEADER_SIZE as i32;

        let mut footer = ChunkFooter {
            flags: FooterFlags::IS_COMPLETED | FooterFlags::IS_MAP_12_BYTES,
            is_completed: true,
            is_map_12bytes: true,
            physical_data_size,
            logical_data_size: physical_data_size as i64,
            map_size: 0,
            hash: [0u8; 16],
        };

        footer.write(buffer);
        self.footer = Some(footer);

        footer
    }

    pub fn update_hash(&mut self, hash: [u8; 16]) {
        if let Some(footer) = self.footer.as_mut() {
            footer.hash = hash;
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChunkManagerBis {
    pub count: usize,
    pub chunks: Vec<ChunkBis>,
    pub writer: u64,
    pub epoch: u64,
}

impl ChunkManagerBis {
    pub fn ongoing_chunk(&self) -> ChunkBis {
        for chunk in &self.chunks {
            if chunk.contains_log_position(self.writer) {
                return *chunk;
            }
        }

        panic!("We got a log position that doesn't belong to any chunk")
    }

    pub fn update_chunk(&mut self, changed: ChunkBis) {
        for chunk in self.chunks.iter_mut() {
            if chunk.num == changed.num {
                *chunk = changed;
            }
        }
    }

    pub fn new_chunk(&mut self) -> ChunkBis {
        let new = ChunkBis::new(self.count);

        self.chunks.push(new);
        self.count += 1;

        new
    }
}

// #[derive(Debug, Copy, Clone)]
pub struct Chunk {
    pub path: PathBuf,
    pub file: File,
    pub header: ChunkHeader,
    pub footer: Option<ChunkFooter>,
}

impl Chunk {
    pub fn new(mut dir: PathBuf, chunk_num: usize) -> io::Result<Self> {
        let path = dir.join(chunk_filename_from(chunk_num, 0));

        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path.as_path())?;

        let header = ChunkHeader {
            file_type: FileType::ChunkFile,
            version: 3,
            chunk_size: CHUNK_SIZE as i32,
            chunk_start_number: chunk_num as i32,
            chunk_end_number: chunk_num as i32,
            is_scavenged: false,
            chunk_id: Uuid::new_v4(),
        };

        file.set_len(CHUNK_FILE_SIZE as u64)?;
        header.write(&mut file)?;
        file.flush()?;

        Ok(Self {
            path,
            file,
            header,
            footer: None,
        })
    }

    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = PathBuf::from(path.as_ref());
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_path())?;

        let header = ChunkHeader::read(&mut file)?;
        let footer = ChunkFooter::read(&mut file)?;

        Ok(Self {
            path,
            file,
            header,
            footer,
        })
    }

    pub fn write(&mut self, content: Bytes) -> io::Result<i64> {
        let length = content.len();
        self.file.write_i32::<LittleEndian>(length as i32)?;
        self.file.write_all(content.as_ref())?;
        self.file.write_i32::<LittleEndian>(length as i32)?;

        let raw_position = self.file.stream_position()?;
        let local_log_position = raw_position - CHUNK_HEADER_SIZE as u64;

        Ok(self.logical_position(local_log_position) as i64)
    }

    pub fn set_cursor_at(&mut self, log_position: u64) -> io::Result<()> {
        let physical_position =
            log_position as u64 - self.start_position() + CHUNK_HEADER_SIZE as u64;

        self.file.seek(SeekFrom::Start(physical_position))?;

        Ok(())
    }

    pub fn read_record(&mut self, buffer: &mut BytesMut) -> io::Result<(RecordHeader, Bytes)> {
        let pre_size = self.file.read_i32::<LittleEndian>()?;
        buffer.reserve(pre_size as usize);

        unsafe {
            buffer.set_len(pre_size as usize);
        }

        self.file
            .read_exact(&mut buffer.as_mut()[..pre_size as usize])?;
        let mut content = buffer.split().freeze();
        let post_size = self.file.read_i32::<LittleEndian>()?;

        if pre_size != post_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Corrupt chunk {} != {} ({}): file {:?}",
                    pre_size,
                    post_size,
                    content.len(),
                    self.path.as_path()
                ),
            ));
        }

        Ok((RecordHeader::new(pre_size as usize, &mut content)?, content))
    }

    pub fn read(&mut self, log_position: i64) -> io::Result<PrepareLog> {
        let physical_position =
            log_position as u64 - self.start_position() + CHUNK_HEADER_SIZE as u64;

        self.file.seek(SeekFrom::Start(physical_position))?;
        let header_size = self.file.read_i32::<LittleEndian>()?;
        let mut buffer = vec![0u8; header_size as usize];
        self.file.read_exact(buffer.as_mut_slice())?;
        let record = PrepareLog::read(1u8, Bytes::from(buffer))?;
        let footer_size = self.file.read_i32::<LittleEndian>()?;

        if header_size != footer_size {
            error!(
                "Record header and footer size don't match! {} != {}",
                header_size, footer_size
            );

            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Record header and footer size don't match!",
            ));
        }

        Ok(record)
    }

    pub fn start_position(&self) -> u64 {
        self.header.chunk_start_number as u64 * CHUNK_SIZE as u64
    }

    pub fn end_position(&self) -> u64 {
        (self.header.chunk_end_number as u64 + 1) * CHUNK_SIZE as u64
    }

    pub fn logical_position(&self, physical_pos: u64) -> u64 {
        physical_pos + self.start_position()
    }

    pub fn md5_hash(&mut self) -> io::Result<[u8; 16]> {
        md5_hash_chunk_file(&mut self.file, self.footer.expect("to be defined"))
    }

    pub fn contains_log_position(&self, log_position: u64) -> bool {
        log_position >= self.start_position() && log_position < self.end_position()
    }
}

pub struct ProposedEvent {
    // #[rune(get, set)]
    // pub id: Uuid,
    pub is_json: bool,
    pub stream_id: String,
    pub event_type: String,
    pub expected_version: i64,
    pub data: Bytes,
    pub metadata: Bytes,
}

#[derive(Debug, Clone)]
pub struct PrepareLog {
    // #[rune(get, set)]
    pub flags: PrepareFlags,
    pub transaction_position: i64,
    pub transaction_offset: i32,
    pub expected_version: i64,
    pub event_stream_id: String,
    pub event_id: uuid::Uuid,
    pub correlation_id: uuid::Uuid,
    pub timestamp: i64,
    pub event_type: String,
    pub data: Bytes,
    pub metadata: Bytes,
}

impl PrepareLog {
    pub fn is_data_json(&self) -> bool {
        self.flags.contains(PrepareFlags::IS_JSON)
    }

    pub fn data_as_json<A: DeserializeOwned>(&self) -> serde_json::Result<A> {
        serde_json::from_slice(self.data.as_ref())
    }

    pub fn size_of_without_raw_bytes(&self, version: u8) -> usize {
        let expected_version_size = if version == 0 { 4 } else { 8 };

        2 + 8
            + 4
            + expected_version_size
            + variable_string_length_bytes_size(self.event_stream_id.len())
            + self.event_stream_id.len()
            + 16
            + 16
            + 8
            + variable_string_length_bytes_size(self.event_type.len())
            + self.event_type.len()
    }

    pub fn read<R>(version: u8, mut src: R) -> io::Result<Self>
    where
        R: Buf,
    {
        let flags =
            PrepareFlags::from_bits(src.get_u16_le()).expect("Invalid prepare flags parsing");
        let transaction_position = src.get_i64_le();
        let transaction_offset = src.get_i32_le();
        let mut expected_version = if version == 0 {
            src.get_i32_le() as i64
        } else {
            src.get_i64_le()
        };

        if version == 0 && expected_version == (i32::MAX as i64) - 1 {
            expected_version = i64::MAX - 1;
        }

        let event_stream_id = read_string(&mut src)?;
        let event_id = read_uuid(&mut src)?;
        let correlation_id = read_uuid(&mut src)?;
        let timestamp = src.get_i64_le();
        let event_type = read_string(&mut src)?;

        let data_len = src.get_i32_le() as usize;
        let data = src.copy_to_bytes(data_len as usize);

        let metadata_len = src.get_i32_le() as usize;
        let metadata = src.copy_to_bytes(metadata_len as usize);

        Ok(PrepareLog {
            flags,
            transaction_offset,
            transaction_position,
            expected_version,
            event_stream_id,
            event_id,
            correlation_id,
            timestamp,
            event_type,
            data,
            metadata,
        })
    }

    pub fn write(self, version: u8, buffer: &mut bytes::BytesMut) {
        buffer.put_u16_le(self.flags.bits());
        buffer.put_i64_le(self.transaction_position);
        buffer.put_i32_le(self.transaction_offset);

        if version == 0 {
            buffer.put_i32_le(self.expected_version as i32);
        } else {
            buffer.put_i64_le(self.expected_version);
        }

        write_string(&self.event_stream_id, buffer);
        buffer.extend_from_slice(self.event_id.as_bytes());
        buffer.extend_from_slice(self.correlation_id.as_bytes());
        buffer.put_i64_le(self.timestamp);
        write_string(&self.event_type, buffer);
        buffer.put_i32_le(self.data.len() as i32);
        buffer.extend_from_slice(self.data.as_ref());
        buffer.put_i32_le(self.metadata.len() as i32);
        buffer.extend_from_slice(self.metadata.as_ref());
    }
}

#[derive(Debug, Copy, Clone)]
pub struct RecordHeader {
    pub size: usize,
    pub r#type: RecordType,
    pub version: u8,
    pub log_position: u64,
}

impl RecordHeader {
    pub fn new(size: usize, bytes: &mut Bytes) -> io::Result<Self> {
        let r#type = match bytes.get_u8() {
            0x00 => RecordType::Prepare,
            0x01 => RecordType::Commit,
            0x02 => RecordType::System,
            x => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unknown record type {}", x),
                ))
            }
        };

        let version = bytes.get_u8();
        let log_position = bytes.get_u64_le();

        Ok(RecordHeader {
            size,
            r#type,
            version,
            log_position,
        })
    }
}

#[derive(Debug, Clone)]
pub enum SystemRecordType {
    Invalid,
    Epoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemRecordFormat {
    Invalid,
    Binary,
    Json,
    Bson,
}

#[derive(Debug, Clone)]
pub struct SystemLog {
    pub timestamp: i64,
    pub kind: SystemRecordType,
    pub format: SystemRecordFormat,
    pub data: Bytes,
}

impl SystemLog {
    pub fn read<R>(mut src: R) -> io::Result<Self>
    where
        R: Buf,
    {
        let timestamp = src.get_i64_le();
        let kind = match src.get_u8() {
            0 => SystemRecordType::Invalid,
            1 => SystemRecordType::Epoch,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Unknown system record type",
                ))
            }
        };
        let format = match src.get_u8() {
            0 => SystemRecordFormat::Invalid,
            1 => SystemRecordFormat::Binary,
            2 => SystemRecordFormat::Json,
            3 => SystemRecordFormat::Bson,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Unknown system record format",
                ))
            }
        };

        // Reserved unused data.
        let _ = src.get_i64_le();
        let data_length = src.get_i32_le() as i64;
        let data = src.copy_to_bytes(data_length as usize);

        Ok(SystemLog {
            timestamp,
            kind,
            format,
            data,
        })
    }

    pub fn write(&self, buf: &mut BytesMut) {
        buf.put_i64_le(self.timestamp);

        match self.kind {
            SystemRecordType::Invalid => buf.put_u8(0),
            SystemRecordType::Epoch => buf.put_u8(1),
        }

        match self.format {
            SystemRecordFormat::Invalid => buf.put_u8(0),
            SystemRecordFormat::Binary => buf.put_u8(1),
            SystemRecordFormat::Json => buf.put_u8(2),
            SystemRecordFormat::Bson => buf.put_u8(3),
        }

        // Reserved unused data.
        buf.put_i64_le(0);
        buf.put_i32_le(self.data.len() as i32);
        buf.put_slice(self.data.as_ref());
    }

    pub fn is_json_format(&self) -> bool {
        self.format == SystemRecordFormat::Json
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct EpochRecord {
    #[serde(rename = "EpochId")]
    pub id: Uuid,
    pub leader_instance_id: Uuid,
    #[serde(rename = "EpochPosition")]
    pub position: i64,
    #[serde(rename = "EpochNumber")]
    pub number: u64,
    #[serde(rename = "PrevEpochPosition")]
    pub previous_position: i64,
    #[serde(rename = "TimeStamp")]
    pub time: DateTime<Utc>,
}

impl EpochRecord {
    pub fn new(
        id: Uuid,
        leader_instance_id: Uuid,
        number: u64,
        position: i64,
        last_epoch_position: i64,
    ) -> Self {
        Self {
            id,
            leader_instance_id,
            position,
            number,
            previous_position: last_epoch_position,
            time: Utc::now(),
        }
    }
}
