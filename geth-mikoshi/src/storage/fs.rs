use std::collections::HashMap;
use std::fs::{read_dir, File, OpenOptions};
use std::io::{self, ErrorKind, Seek};
#[cfg(target_family = "unix")]
use std::os::unix::fs::FileExt;
#[cfg(target_os = "windows")]
use std::os::windows::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};

use crate::constants::CHUNK_SIZE;
use crate::storage::{FileCategory, FileId, Storage};

#[derive(Clone, Debug)]
pub struct FileSystemStorage {
    root: PathBuf,
    buffer: BytesMut,
    inner: Arc<Mutex<HashMap<FileId, Arc<File>>>>,
}

impl FileSystemStorage {
    pub fn new_storage(root: PathBuf) -> io::Result<Storage> {
        std::fs::create_dir_all(root.as_path())?;

        Ok(Storage::FileSystem(Self {
            root,
            buffer: BytesMut::default(),
            inner: Arc::new(Mutex::new(Default::default())),
        }))
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
            .truncate(false)
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

    pub fn root(&self) -> &Path {
        self.root.as_path()
    }
}

impl FileSystemStorage {
    pub fn write_to(&self, id: FileId, offset: u64, bytes: Bytes) -> io::Result<()> {
        let file = self.load_or_create(id)?;

        #[cfg(target_family = "unix")]
        file.write_all_at(&bytes, offset)?;
        #[cfg(target_os = "windows")]
        win_write_all(&file, &bytes, offset)?;
        file.sync_all()?;

        Ok(())
    }

    pub fn append(&self, id: FileId, bytes: Bytes) -> io::Result<()> {
        let mut file = self.load_or_create(id)?;

        let offset = file.seek(io::SeekFrom::End(0))?;

        #[cfg(target_family = "unix")]
        file.write_all_at(&bytes, offset)?;
        #[cfg(target_os = "windows")]
        win_write_all(&file, &bytes, offset)?;
        file.sync_all()?;

        Ok(())
    }

    pub fn offset(&self, id: FileId) -> io::Result<u64> {
        let mut file = self.load_or_create(id)?;
        file.seek(io::SeekFrom::End(0))
    }

    pub fn read_from(&self, id: FileId, offset: u64, len: usize) -> io::Result<Bytes> {
        let file = self.load_or_create(id)?;
        let mut buffer = self.buffer.clone();
        buffer.resize(len, 0);
        #[cfg(target_family = "unix")]
        file.read_exact_at(&mut buffer, offset)?;
        #[cfg(target_os = "windows")]
        win_read_exact(&file, &mut buffer, offset)?;

        Ok(buffer.freeze())
    }

    pub fn read_all(&self, id: FileId) -> io::Result<Bytes> {
        let file = self.load_or_create(id)?;
        let mut buffer = self.buffer.clone();
        let len = file.metadata()?.len() as usize;

        buffer.resize(len, 0);
        #[cfg(target_family = "unix")]
        file.read_exact_at(&mut buffer, 0)?;
        #[cfg(target_os = "windows")]
        win_read_exact(&file, &mut buffer, 0)?;

        Ok(buffer.freeze())
    }

    pub fn exists(&self, id: FileId) -> io::Result<bool> {
        match std::fs::metadata(self.file_path(id)) {
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
            Ok(_) => Ok(true),
        }
    }

    pub fn remove(&self, id: FileId) -> io::Result<()> {
        std::fs::remove_file(self.file_path(id))
    }

    pub fn len(&self, id: FileId) -> io::Result<usize> {
        Ok(std::fs::metadata(self.file_path(id))?.len() as usize)
    }

    pub fn list<C>(&self, category: C) -> io::Result<Vec<C::Item>>
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

#[cfg(target_os = "windows")]
fn win_write_all(file: &File, bytes: &Bytes, mut offset: u64) -> io::Result<()> {
    let mut buffer = bytes.as_ref();

    while !buffer.is_empty() {
        let written = file.seek_write(buffer, offset)?;
        buffer = &buffer[written..];
        offset += written as u64;
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn win_read_exact(file: &File, mut buffer: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buffer.is_empty() {
        let read = file.seek_read(buffer, offset)?;

        if read == 0 {
            break;
        }

        buffer = &mut buffer[read..];
        offset += read as u64;
    }

    Ok(())
}
