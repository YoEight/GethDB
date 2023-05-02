pub mod fs;
pub mod in_mem;

use bytes::buf::UninitSlice;
use bytes::{BufMut, Bytes, BytesMut};
use std::io;
use std::sync::RwLock;
use uuid::Uuid;

#[derive(Copy, Clone)]
pub enum FileType {
    SSTable(Uuid),
    IndexMap,
}

pub trait Storage: Clone {
    fn write_to(&self, r#type: FileType, bytes: Bytes) -> io::Result<()>;
    fn read_from(&self, r#type: FileType, offset: u64, len: usize) -> io::Result<Bytes>;
    fn read_all(&self, r#type: FileType) -> io::Result<Bytes>;
    fn exists(&self, r#type: FileType) -> io::Result<bool>;
    fn remove(&self, r#type: FileType) -> io::Result<()>;
    fn len(&self, r#type: FileType) -> io::Result<usize>;
}
