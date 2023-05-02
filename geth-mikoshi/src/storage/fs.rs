use crate::storage::{FileId, Storage};
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Clone)]
pub struct FileSystemStorage {
    root: PathBuf,
    buffer: BytesMut,
    inner: Arc<Mutex<HashMap<Uuid, Arc<File>>>>,
}

impl FileSystemStorage {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            buffer: BytesMut::default(),
            inner: Arc::new(Mutex::new(Default::default())),
        }
    }

    fn load_or_create(&self, id: Uuid) -> io::Result<Arc<File>> {
        let mut inner = self.inner.lock().unwrap();
        let file = if let Some(file) = inner.get(&id) {
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
            .open(self.root.join(path))?;

        Ok(Arc::new(file))
    }

    fn file_path(&self, id: FileId) -> PathBuf {
        match id {
            FileId::SSTable(id) => self.root.join(id.to_string()),
            FileId::IndexMap => self.root.join("indexmap"),
        }
    }
}

impl Storage for FileSystemStorage {
    fn write_to(&self, id: FileId, bytes: Bytes) -> io::Result<()> {
        let file = match id {
            FileId::SSTable(id) => self.load_or_create(id),
            FileId::IndexMap => self.open_file("indexmap"),
        }?;

        file.write_all_at(&bytes, 0)?;
        file.sync_all()?;

        Ok(())
    }

    fn read_from(&self, id: FileId, offset: u64, len: usize) -> io::Result<Bytes> {
        let file = match id {
            FileId::SSTable(id) => self.load_or_create(id),
            FileId::IndexMap => self.open_file("indexmap"),
        }?;

        let mut buffer = self.buffer.clone();
        buffer.resize(len, 0);
        file.read_exact_at(&mut buffer, offset)?;

        Ok(buffer.freeze())
    }

    fn read_all(&self, id: FileId) -> io::Result<Bytes> {
        let file = match id {
            FileId::SSTable(id) => self.load_or_create(id),
            FileId::IndexMap => self.open_file("indexmap"),
        }?;

        let mut buffer = self.buffer.clone();
        let len = file.metadata()?.len() as usize;

        buffer.resize(len, 0);
        file.read_exact_at(&mut buffer, 0)?;

        Ok(buffer.freeze())
    }

    fn exists(&self, id: FileId) -> io::Result<bool> {
        let meta = match id {
            FileId::SSTable(id) => std::fs::metadata(self.root.join(id.to_string())),
            FileId::IndexMap => std::fs::metadata(self.root.join("indexmap")),
        };

        match meta {
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
            Ok(_) => Ok(true),
        }
    }

    fn remove(&self, id: FileId) -> io::Result<()> {
        std::fs::remove_file(self.file_path(id))
    }

    fn len(&self, id: FileId) -> io::Result<usize> {
        Ok(std::fs::metadata(self.file_path(id))?.len() as usize)
    }
}
