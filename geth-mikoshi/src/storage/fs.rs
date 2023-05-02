use crate::storage::{FileType, Storage};
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::ErrorKind;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Clone)]
pub struct FileSystemStorage {
    root: PathBuf,
    buffer: BytesMut,
    inner: Mutex<HashMap<Uuid, Arc<File>>>,
}

impl FileSystemStorage {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            buffer: BytesMut::default(),
            inner: Mutex::new(Default::default()),
        }
    }

    fn load_or_create(&self, id: Uuid) -> io::Result<Arc<File>> {
        let mut inner = self.inner.lock().unwrap();
        let file = if let Some(file) = self.inner.get(&id) {
            file.clone()
        } else {
            let file = self.open_file(id.to_string())?;
            inner.insert(id, file.clone());

            file
        };

        Ok(file)
    }

    fn open_file(&self, path: impl AsRef<Path>) -> io::Result<Arc<File>> {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(self.root.join(id.to_string()))?;

        Ok(Arc::new(file))
    }

    fn file_path(&self, id: FileType) -> PathBuf {
        match r#type {
            FileType::SSTable(id) => self.root.join(id.to_string()),
            FileType::IndexMap => self.root.join("indexmap"),
        }
    }
}

impl Storage for FileSystemStorage {
    fn write_to(&self, r#type: FileType, bytes: Bytes) -> std::io::Result<()> {
        let file = match r#type {
            FileType::SSTable(id) => self.load_or_create(id),
            FileType::IndexMap => self.open_file("indexmap"),
        }?;

        file.write_all_at(&bytes, 0)?;
        file.sync_all()?;

        Ok(())
    }

    fn read_from(&self, r#type: FileType, offset: u64, len: usize) -> std::io::Result<Bytes> {
        let file = match r#type {
            FileType::SSTable(id) => self.load_or_create(id),
            FileType::IndexMap => self.open_file("indexmap"),
        }?;

        let mut buffer = self.buffer.clone();
        buffer.resize(len, 0);
        file.read_exact_at(&mut buffer, offset)?;

        Ok(buffer.freeze())
    }

    fn read_all(&self, r#type: FileType) -> std::io::Result<Bytes> {
        let file = match r#type {
            FileType::SSTable(id) => self.load_or_create(id),
            FileType::IndexMap => self.open_file("indexmap"),
        }?;

        let mut buffer = self.buffer.clone();
        let len = file.metadata()?.len() as usize;

        buffer.resize(len, 0);
        file.read_exact_at(&mut buffer, 0)?;

        Ok(buffer.freeze())
    }

    fn exists(&self, r#type: FileType) -> std::io::Result<bool> {
        let meta = match r#type {
            FileType::SSTable(id) => std::fs::metadata(self.root.join(id.to_string())),
            FileType::IndexMap => std::fs::metadata(self.root.join("indexmap")),
        };

        match meta {
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
            Ok(_) => Ok(true),
        }
    }

    fn remove(&self, r#type: FileType) -> std::io::Result<()> {
        std::fs::remove_file(self.file_path(r#type))
    }

    fn len(&self, r#type: FileType) -> std::io::Result<usize> {
        Ok(std::fs::metadata(self.file_path(r#type))?.len() as usize)
    }
}
