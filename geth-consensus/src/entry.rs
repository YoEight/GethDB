use bytes::Bytes;

#[derive(Debug, Copy, Clone, Default)]
pub struct EntryId {
    pub index: u64,
    pub term: u64,
}

impl EntryId {
    pub fn new(index: u64, term: u64) -> Self {
        Self { index, term }
    }
}

#[derive(Debug)]
pub struct Entry {
    pub index: u64,
    pub term: u64,
    pub payload: Bytes,
}
