use std::{fmt, io};

use bytes::{BufMut, Bytes, BytesMut};
use uuid::Uuid;

pub use fs::FileSystemStorage;
pub use in_mem::InMemoryStorage;

pub(crate) mod fs;
pub(crate) mod in_mem;

#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum FileId {
    SSTable(Uuid),
    IndexMap,
    Chunk { num: usize, version: usize },
    Checkpoint(Checkpoint),
}

impl FileId {
    pub fn ss_table(id: Uuid) -> Self {
        Self::SSTable(id)
    }

    pub fn chunk(num: usize, version: usize) -> Self {
        Self::Chunk { num, version }
    }

    pub const fn writer_chk() -> Self {
        Self::Checkpoint(Checkpoint::Writer)
    }

    pub const fn index_chk() -> Self {
        Self::Checkpoint(Checkpoint::Index)
    }

    pub const fn index_global_chk() -> Self {
        Self::Checkpoint(Checkpoint::IndexGlobal)
    }
}

impl fmt::Debug for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileId::SSTable(uuid) => write!(f, "ss_table-{uuid}"),
            FileId::IndexMap => write!(f, "indexmap"),
            FileId::Chunk { num, version } => write!(f, "chunk-{num:06}.{version:06}"),
            FileId::Checkpoint(checkpoint) => write!(f, "{}", checkpoint.as_str()),
        }
    }
}

#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Checkpoint {
    Writer,
    Index,
    IndexGlobal,
}

impl Checkpoint {
    pub fn as_str(&self) -> &'static str {
        match self {
            Checkpoint::Writer => "writer.chk",
            Checkpoint::Index => "index.chk",
            Checkpoint::IndexGlobal => "index_global.chk",
        }
    }

    pub fn file_id(&self) -> FileId {
        FileId::Checkpoint(*self)
    }
}

pub trait FileCategory {
    type Item;

    fn parse(&self, name: &str) -> Option<Self::Item>;
}

#[derive(Clone, Debug)]
pub enum Storage {
    FileSystem(FileSystemStorage),
    InMemory(InMemoryStorage),
}

impl Storage {
    pub fn write_to(&self, id: FileId, offset: u64, bytes: Bytes) -> io::Result<()> {
        match self {
            Storage::FileSystem(s) => s.write_to(id, offset, bytes),
            Storage::InMemory(s) => s.write_to(id, offset, bytes),
        }
    }

    pub fn append(&self, id: FileId, bytes: Bytes) -> io::Result<()> {
        match self {
            Storage::FileSystem(s) => s.append(id, bytes),
            Storage::InMemory(s) => s.append(id, bytes),
        }
    }

    pub fn offset(&self, id: FileId) -> io::Result<u64> {
        match self {
            Storage::FileSystem(s) => s.offset(id),
            Storage::InMemory(s) => s.offset(id),
        }
    }

    pub fn read_from(&self, id: FileId, offset: u64, len: usize) -> io::Result<Bytes> {
        match self {
            Storage::FileSystem(s) => s.read_from(id, offset, len),
            Storage::InMemory(s) => s.read_from(id, offset, len),
        }
    }

    pub fn read_all(&self, id: FileId) -> io::Result<Bytes> {
        match self {
            Storage::FileSystem(s) => s.read_all(id),
            Storage::InMemory(s) => s.read_all(id),
        }
    }

    pub fn exists(&self, id: FileId) -> io::Result<bool> {
        match self {
            Storage::FileSystem(s) => s.exists(id),
            Storage::InMemory(s) => s.exists(id),
        }
    }

    pub fn remove(&self, id: FileId) -> io::Result<()> {
        match self {
            Storage::FileSystem(s) => s.remove(id),
            Storage::InMemory(s) => s.remove(id),
        }
    }

    pub fn len(&self, id: FileId) -> io::Result<usize> {
        match self {
            Storage::FileSystem(s) => s.len(id),
            Storage::InMemory(s) => s.len(id),
        }
    }

    pub fn list<C>(&self, category: C) -> io::Result<Vec<C::Item>>
    where
        C: FileCategory,
    {
        match self {
            Storage::FileSystem(s) => s.list(category),
            Storage::InMemory(s) => s.list(category),
        }
    }

    pub fn init(&self) -> eyre::Result<()> {
        if !self.exists(FileId::writer_chk())? {
            let mut buffer = BytesMut::new();
            buffer.put_u64_le(0);
            self.write_to(FileId::writer_chk(), 0, buffer.freeze())?;
        }

        Ok(())
    }
}
