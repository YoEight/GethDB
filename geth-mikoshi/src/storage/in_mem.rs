use crate::storage::{FileId, Storage};
use bytes::{Buf, Bytes};
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Clone)]
pub struct InMemoryStorage {
    inner: Arc<Mutex<HashMap<Uuid, Bytes>>>,
    indexmap: Arc<Mutex<Bytes>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Default::default())),
            indexmap: Arc::new(Default::default()),
        }
    }
}

impl Storage for InMemoryStorage {
    fn write_to(&self, id: FileId, bytes: Bytes) -> io::Result<()> {
        match id {
            FileId::SSTable(id) => {
                let mut inner = self.inner.lock().unwrap();
                inner.insert(id, bytes);
            }

            FileId::IndexMap => {
                let mut indexmap = self.indexmap.lock().unwrap();
                *indexmap = bytes;
            }
        }

        Ok(())
    }

    fn read_from(&self, id: FileId, offset: u64, len: usize) -> io::Result<Bytes> {
        let mut bytes = match id {
            FileId::SSTable(id) => {
                let mut inner = self.inner.lock().unwrap();

                if let Some(bytes) = inner.get(&id) {
                    bytes.clone()
                } else {
                    Bytes::new()
                }
            }

            FileId::IndexMap => self.indexmap.lock().unwrap().clone(),
        };

        bytes.advance(offset as usize);
        Ok(bytes.copy_to_bytes(len))
    }

    fn read_all(&self, id: FileId) -> io::Result<Bytes> {
        Ok(match id {
            FileId::SSTable(id) => {
                let mut inner = self.inner.lock().unwrap();

                if let Some(bytes) = inner.get(&id) {
                    bytes.clone()
                } else {
                    Bytes::new()
                }
            }

            FileId::IndexMap => self.indexmap.lock().unwrap().clone(),
        })
    }

    fn exists(&self, id: FileId) -> io::Result<bool> {
        match id {
            FileId::SSTable(id) => {
                let inner = self.inner.lock().unwrap();
                Ok(inner.contains_key(&id))
            }

            FileId::IndexMap => {
                let indexmap = self.indexmap.lock().unwrap();
                Ok(!indexmap.is_empty())
            }
        }
    }

    fn remove(&self, id: FileId) -> io::Result<()> {
        match id {
            FileId::SSTable(id) => {
                let mut inner = self.inner.lock().unwrap();
                inner.remove(&id);
            }

            FileId::IndexMap => {
                let mut indexmap = self.indexmap.lock().unwrap();
                *indexmap = Bytes::new();
            }
        }

        Ok(())
    }

    fn len(&self, id: FileId) -> io::Result<usize> {
        match id {
            FileId::SSTable(id) => {
                let inner = self.inner.lock().unwrap();
                Ok(inner.get(&id).map(|b| b.len()).unwrap_or_default())
            }

            FileId::IndexMap => {
                let indexmap = self.indexmap.lock().unwrap();
                Ok(indexmap.len())
            }
        }
    }
}
