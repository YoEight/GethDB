use bytes::Bytes;

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct EntryId {
    pub index: u64,
    pub term: u64,
}

impl EntryId {
    pub fn new(index: u64, term: u64) -> Self {
        Self { index, term }
    }
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub index: u64,
    pub term: u64,
    pub payload: Bytes,
}

impl Entry {
    pub fn id(&self) -> EntryId {
        EntryId {
            index: self.index,
            term: self.term,
        }
    }
}
