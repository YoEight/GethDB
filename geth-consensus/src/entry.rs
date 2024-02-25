use bytes::Bytes;

#[derive(Debug, Copy, Clone, Default)]
pub struct EntryId {
    pub index: u64,
    pub term: u64,
}

#[derive(Debug)]
pub struct Entry {
    pub index: u64,
    pub term: u64,
    pub payload: Bytes,
}
