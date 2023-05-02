use crate::storage::{FileType, Storage};
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
    fn write_to(&self, r#type: FileType, bytes: Bytes) -> io::Result<()> {
        match r#type {
            FileType::SSTable(id) => {
                let mut inner = self.inner.lock().unwrap();
                inner.insert(id, bytes);
            }

            FileType::IndexMap => {
                let mut indexmap = self.indexmap.lock().unwrap();
                *indexmap = bytes;
            }
        }

        Ok(())
    }

    fn read_from(&self, r#type: FileType, offset: u64, len: usize) -> io::Result<Bytes> {
        let mut bytes = match r#type {
            FileType::SSTable(id) => {
                let mut inner = self.inner.lock().unwrap();

                if let Some(bytes) = inner.get(&id) {
                    bytes.clone()
                } else {
                    Bytes::new()
                }
            }

            FileType::IndexMap => self.indexmap.lock().unwrap().clone(),
        };

        bytes.advance(offset as usize);
        Ok(bytes.copy_to_bytes(len))
    }

    fn read_all(&self, r#type: FileType) -> io::Result<Bytes> {
        Ok(match r#type {
            FileType::SSTable(id) => {
                let mut inner = self.inner.lock().unwrap();

                if let Some(bytes) = inner.get(&id) {
                    bytes.clone()
                } else {
                    Bytes::new()
                }
            }

            FileType::IndexMap => self.indexmap.lock().unwrap().clone(),
        })
    }

    fn exists(&self, r#type: FileType) -> io::Result<bool> {
        match r#type {
            FileType::SSTable(id) => {
                let inner = self.inner.lock().unwrap();
                Ok(inner.contains_key(&id))
            }

            FileType::IndexMap => {
                let indexmap = self.indexmap.lock().unwrap();
                Ok(!indexmap.is_empty())
            }
        }
    }

    fn remove(&self, r#type: FileType) -> io::Result<()> {
        match r#type {
            FileType::SSTable(id) => {
                let mut inner = self.inner.lock().unwrap();
                inner.remove(&id);
            }

            FileType::IndexMap => {
                let mut indexmap = self.indexmap.lock().unwrap();
                *indexmap = Bytes::new();
            }
        }

        Ok(())
    }

    fn len(&self, r#type: FileType) -> io::Result<usize> {
        match r#type {
            FileType::SSTable(id) => {
                let inner = self.inner.lock().unwrap();
                Ok(inner.get(&id).map(|b| b.len()).unwrap_or_default())
            }

            FileType::IndexMap => {
                let indexmap = self.indexmap.lock().unwrap();
                Ok(indexmap.len())
            }
        }
    }
}
