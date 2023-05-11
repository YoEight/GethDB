use crate::constants::CHUNK_SIZE;
use crate::storage::{FileCategory, FileId, Storage};
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::fs::{read_dir, File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct FileSystemStorage {
    root: PathBuf,
    buffer: BytesMut,
    inner: Arc<Mutex<HashMap<FileId, Arc<File>>>>,
}

impl FileSystemStorage {
    pub fn new(root: PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(root.as_path())?;

        Ok(Self {
            root,
            buffer: BytesMut::default(),
            inner: Arc::new(Mutex::new(Default::default())),
        })
    }

    fn load_or_create(&self, id: FileId) -> io::Result<Arc<File>> {
        let mut inner = self.inner.lock().unwrap();
        let file = if let Some(file) = inner.get(&id) {
            file.clone()
        } else {
            let path = self.file_path(id);
            let file = self.open_file(path)?;

            if let FileId::Chunk { .. } = id {
                file.set_len(CHUNK_SIZE as u64)?;
            }

            let file = Arc::new(file);
            inner.insert(id, file.clone());

            file
        };

        Ok(file)
    }

    fn open_file(&self, path: impl AsRef<Path>) -> io::Result<File> {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path)?;

        Ok(file)
    }

    fn file_path(&self, id: FileId) -> PathBuf {
        match id {
            FileId::SSTable(id) => self.root.join(id.to_string()),
            FileId::IndexMap => self.root.join("indexmap"),
            FileId::Chunk { num, version } => self.root.join(chunk_filename_from(num, version)),
            FileId::Checkpoint(c) => self.root.join(c.as_str()),
        }
    }
}

impl Storage for FileSystemStorage {
    fn write_to(&self, id: FileId, offset: u64, bytes: Bytes) -> io::Result<()> {
        let file = self.load_or_create(id)?;

        file.write_all_at(&bytes, offset)?;
        file.sync_all()?;

        Ok(())
    }

    fn read_from(&self, id: FileId, offset: u64, len: usize) -> io::Result<Bytes> {
        let file = self.load_or_create(id)?;
        let mut buffer = self.buffer.clone();
        buffer.resize(len, 0);
        file.read_exact_at(&mut buffer, offset)?;

        Ok(buffer.freeze())
    }

    fn read_all(&self, id: FileId) -> io::Result<Bytes> {
        let file = self.load_or_create(id)?;
        let mut buffer = self.buffer.clone();
        let len = file.metadata()?.len() as usize;

        buffer.resize(len, 0);
        file.read_exact_at(&mut buffer, 0)?;

        Ok(buffer.freeze())
    }

    fn exists(&self, id: FileId) -> io::Result<bool> {
        match std::fs::metadata(self.file_path(id)) {
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

    fn list<C>(&self, category: C) -> io::Result<Vec<C::Item>>
    where
        C: FileCategory,
    {
        let mut entries = read_dir(self.root.as_path())?;
        let mut result = Vec::new();

        while let Some(entry) = entries.next().transpose()? {
            if let Some(filename) = entry.file_name().to_str() {
                if let Some(item) = category.parse(filename) {
                    result.push(item);
                }
            }
        }

        Ok(result)
    }
}

fn chunk_filename_from(seq_number: usize, version: usize) -> String {
    format!("chunk-{:06}.{:06}", seq_number, version)
}
