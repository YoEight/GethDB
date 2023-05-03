pub mod fs;
pub mod in_mem;

use bytes::Bytes;
use std::io;
use uuid::Uuid;

#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum FileId {
    SSTable(Uuid),
    IndexMap,
    Chunk { num: usize, version: usize },
}

pub trait FileCategory {
    type Item;

    fn parse(&self, name: &str) -> Option<Self::Item>;
}

pub trait Storage: Clone {
    fn write_to(&self, id: FileId, offset: u64, bytes: Bytes) -> io::Result<()>;
    fn read_from(&self, id: FileId, offset: u64, len: usize) -> io::Result<Bytes>;
    fn read_all(&self, id: FileId) -> io::Result<Bytes>;
    fn exists(&self, id: FileId) -> io::Result<bool>;
    fn remove(&self, id: FileId) -> io::Result<()>;
    fn len(&self, id: FileId) -> io::Result<usize>;

    fn list<C>(&self, category: C) -> io::Result<Vec<C::Item>>
    where
        C: FileCategory;
}
