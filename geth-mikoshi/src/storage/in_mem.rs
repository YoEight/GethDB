use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

use bytes::{BufMut, Bytes, BytesMut};

use crate::constants::CHUNK_SIZE;
use crate::storage::{FileCategory, FileId, Storage};

struct Internal {
    buffer: BytesMut,
    map: HashMap<FileId, BytesMut>,
}

impl Default for Internal {
    fn default() -> Self {
        Self {
            buffer: BytesMut::new(),
            map: Default::default(),
        }
    }
}

#[derive(Clone)]
pub struct InMemoryStorage {
    inner: Arc<Mutex<Internal>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        InMemoryStorage::default()
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Default::default())),
        }
    }
}

impl Storage for InMemoryStorage {
    fn write_to(&self, id: FileId, offset: u64, bytes: Bytes) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let offset = offset as usize;

        if let Some(buffer) = inner.map.get_mut(&id) {
            match buffer.len().cmp(&offset) {
                std::cmp::Ordering::Equal => {
                    buffer.extend_from_slice(&bytes);
                }

                std::cmp::Ordering::Greater => {
                    if offset + bytes.len() > buffer.len() {
                        let lower_part = buffer.len() - offset;
                        let additional_bytes = (offset + bytes.len()) - buffer.len();
                        buffer[offset..lower_part]
                            .copy_from_slice(bytes.slice(0..lower_part).as_ref());
                        buffer.reserve(additional_bytes);
                        buffer.put(bytes.slice(lower_part..));
                    } else {
                        buffer[offset..offset + bytes.len()].copy_from_slice(&bytes);
                    }
                }

                std::cmp::Ordering::Less => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "write_to: offset exceed current byte buffer",
                    ))
                }
            }
        } else {
            if let FileId::Chunk { .. } = id {
                inner.buffer.resize(CHUNK_SIZE, 0);
                inner.buffer[offset..offset + bytes.len()].copy_from_slice(&bytes);
            } else {
                if offset != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "write_to: offset exceed current byte buffer",
                    ));
                }

                inner.buffer.extend_from_slice(&bytes);
            };

            let new_buffer = inner.buffer.split();

            inner.map.insert(id, new_buffer);
        }

        Ok(())
    }

    fn append(&self, id: FileId, bytes: Bytes) -> io::Result<()> {
        if let FileId::Chunk { .. } = id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "append: chunk files do not support append operation",
            ));
        }

        let mut inner = self.inner.lock().unwrap();

        if let Some(buffer) = inner.map.get_mut(&id) {
            buffer.extend_from_slice(&bytes);
        } else {
            inner.buffer.extend_from_slice(&bytes);
            let new_buffer = inner.buffer.split();

            inner.map.insert(id, new_buffer);
        }

        Ok(())
    }

    fn offset(&self, id: FileId) -> io::Result<u64> {
        if let FileId::Chunk { .. } = id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset: chunk files do not support offset operation",
            ));
        }

        let inner = self.inner.lock().unwrap();

        if let Some(buffer) = inner.map.get(&id) {
            return Ok(buffer.len() as u64);
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("offset: file {:?} does not exist", id),
        ))
    }

    fn read_from(&self, id: FileId, offset: u64, len: usize) -> io::Result<Bytes> {
        let inner = self.inner.lock().unwrap();

        if let Some(buffer) = inner.map.get(&id) {
            let offset = offset as usize;

            if (offset + len) > buffer.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "read_from: range exceeds current byte buffer",
                ));
            }

            return Ok(Bytes::copy_from_slice(&buffer[offset..offset + len]));
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("read_from: {:?} file doesnt exist", id),
        ))
    }

    fn read_all(&self, id: FileId) -> io::Result<Bytes> {
        let inner = self.inner.lock().unwrap();

        if let Some(buffer) = inner.map.get(&id) {
            return Ok(Bytes::copy_from_slice(&buffer[..]));
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("read_all: {:?} file doesnt exist", id),
        ))
    }

    fn exists(&self, id: FileId) -> io::Result<bool> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.map.contains_key(&id))
    }

    fn remove(&self, id: FileId) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.map.remove(&id);

        Ok(())
    }

    fn len(&self, id: FileId) -> io::Result<usize> {
        let inner = self.inner.lock().unwrap();

        if let Some(buffer) = inner.map.get(&id) {
            return Ok(buffer.len());
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("len: {:?} file doesnt exist", id),
        ))
    }

    fn list<C>(&self, _category: C) -> io::Result<Vec<C::Item>>
    where
        C: FileCategory,
    {
        Ok(Vec::new())
    }
}
