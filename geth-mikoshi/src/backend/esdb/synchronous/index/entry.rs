use std::cmp::Ordering;

#[derive(Debug, Clone, Copy)]
pub struct IndexEntry {
    pub stream: u64,
    pub version: i64,
    pub position: i64,
}

impl PartialOrd<Self> for IndexEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for IndexEntry {}

impl PartialEq<Self> for IndexEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Ord for IndexEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut key_cmp = self.stream.cmp(&other.stream);

        if key_cmp != Ordering::Equal {
            return key_cmp;
        }

        key_cmp = self.version.cmp(&other.version);

        if key_cmp != Ordering::Equal {
            return key_cmp;
        }

        self.position.cmp(&other.position)
    }
}
