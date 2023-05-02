pub mod fs;
pub mod in_mem;

use bytes::Bytes;
use std::io;
use uuid::Uuid;

#[derive(Copy, Clone)]
pub enum FileId {
    SSTable(Uuid),
    IndexMap,
}

pub trait Storage: Clone {
    fn write_to(&self, id: FileId, bytes: Bytes) -> io::Result<()>;
    fn read_from(&self, id: FileId, offset: u64, len: usize) -> io::Result<Bytes>;
    fn read_all(&self, id: FileId) -> io::Result<Bytes>;
    fn exists(&self, id: FileId) -> io::Result<bool>;
    fn remove(&self, id: FileId) -> io::Result<()>;
    fn len(&self, id: FileId) -> io::Result<usize>;
}
