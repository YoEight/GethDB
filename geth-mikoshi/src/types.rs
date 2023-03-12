use bitflags::bitflags;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::BufMut;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use uuid::Uuid;

const CHUNK_SIZE: usize = 256 * 1024 * 1024;
const CHUNK_FOOTER_SIZE: usize = 128;
const CHUNK_HEADER_SIZE: usize = 128;

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
pub struct Chunk {
    header: ChunkHeader,
    footer: Option<ChunkFooter>,
}

impl Chunk {
    fn new(chunk_num: usize) -> Self {
        let header = ChunkHeader {
            file_type: FileType::ChunkFile,
            version: 3,
            chunk_size: CHUNK_SIZE as i32,
            chunk_start_number: chunk_num as i32,
            chunk_end_number: chunk_num as i32,
            is_scavenged: false,
            chunk_id: uuid::Uuid::new_v4(),
        };

        Self {
            header,
            footer: None,
        }
    }

    fn start_position(&self) -> u64 {
        self.header.chunk_start_number as u64 * CHUNK_SIZE as u64
    }

    fn logical_position(&self, physical_pos: u64) -> u64 {
        physical_pos + self.start_position()
    }
}
