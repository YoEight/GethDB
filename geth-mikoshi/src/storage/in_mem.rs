use crate::storage::{FileCategory, FileId, Storage};
use bytes::{Buf, Bytes};
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Clone)]
pub struct InMemoryStorage {
    inner: Arc<Mutex<HashMap<FileId, Bytes>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Default::default())),
        }
    }
}

impl Storage for InMemoryStorage {
    fn write_to(&self, id: FileId, offset: u64, bytes: Bytes) -> io::Result<()> {
        match &id {
            FileId::SSTable(_) | FileId::IndexMap => {
                let mut inner = self.inner.lock().unwrap();
                inner.insert(id, bytes);
            }

            _ => {}
        }

        Ok(())
    }

    fn read_from(&self, id: FileId, offset: u64, len: usize) -> io::Result<Bytes> {
        let mut bytes = {
            let mut inner = self.inner.lock().unwrap();
            inner.get(&id).cloned().unwrap_or_default()
        };

        bytes.advance(offset as usize);
        Ok(bytes.copy_to_bytes(len))
    }

    fn read_all(&self, id: FileId) -> io::Result<Bytes> {
        let mut inner = self.inner.lock().unwrap();

        Ok(inner.get(&id).cloned().unwrap_or_default())
    }

    fn exists(&self, id: FileId) -> io::Result<bool> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.contains_key(&id))
    }

    fn remove(&self, id: FileId) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.remove(&id);

        Ok(())
    }

    fn len(&self, id: FileId) -> io::Result<usize> {
        let inner = self.inner.lock().unwrap();

        Ok(inner.get(&id).map(|b| b.len()).unwrap_or_default())
    }

    fn list<C>(&self, category: C) -> io::Result<Vec<C::Item>>
    where
        C: FileCategory,
    {
        Ok(Vec::new())
    }
}
