use geth_common::ExpectedRevision;

#[derive(Copy, Clone, Debug)]
pub enum CurrentRevision {
    NoStream,
    Revision(u64),
}

impl CurrentRevision {
    pub fn next_revision(self) -> u64 {
        match self {
            CurrentRevision::NoStream => 0,
            CurrentRevision::Revision(r) => r + 1,
        }
    }

    pub fn as_expected(self) -> ExpectedRevision {
        match self {
            CurrentRevision::NoStream => ExpectedRevision::NoStream,
            CurrentRevision::Revision(v) => ExpectedRevision::Revision(v),
        }
    }

    pub fn is_deleted(&self) -> bool {
        if let CurrentRevision::Revision(r) = self {
            return *r == u64::MAX;
        }

        false
    }

    pub fn revision(self) -> Option<u64> {
        if let CurrentRevision::Revision(r) = self {
            return Some(r);
        }

        None
    }
}
