mod proc;

use bytes::{Bytes, BytesMut};
use geth_common::ExpectedRevision;
pub use proc::Writing;

enum Request {
    Append {
        ident: Bytes,
        expected: ExpectedRevision,
        events: Bytes,
    },

    Delete {
        ident: Bytes,
        expected: ExpectedRevision,
    },
}

impl Request {
    fn try_from(bytes: Bytes) -> Option<Self> {
        todo!()
    }

    fn ident(&self) -> &Bytes {
        match self {
            Request::Append { ident, .. } => ident,
            Request::Delete { ident, .. } => ident,
        }
    }

    fn expected(&self) -> ExpectedRevision {
        match self {
            Request::Append { expected, .. } => *expected,
            Request::Delete { expected, .. } => *expected,
        }
    }
}

enum Response {
    WrongExpectedRevision {
        expected: ExpectedRevision,
        current: ExpectedRevision,
    },

    Error,
}

impl Response {
    fn wrong_expected_revision(expected: ExpectedRevision, current: ExpectedRevision) -> Self {
        Self::WrongExpectedRevision { expected, current }
    }

    fn error() -> Self {
        Self::Error
    }

    fn serialize(self, bytes: &mut BytesMut) -> Bytes {
        todo!()
    }
}
