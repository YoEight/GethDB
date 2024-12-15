mod proc;

use bytes::{Buf, Bytes, BytesMut};
use geth_common::ExpectedRevision;
pub use proc::Writing;

enum Request {
    Append {
        ident: Bytes,
        expected: ExpectedRevision,
        events: Bytes,
    },
}

impl Request {
    fn try_from(mut bytes: Bytes) -> Option<Self> {
        let len = bytes.get_u16_le() as usize;
        let ident = bytes.copy_to_bytes(len);

        let expected = match bytes.get_u8() {
            0x00 => ExpectedRevision::Any,
            0x01 => ExpectedRevision::NoStream,
            0x02 => ExpectedRevision::StreamExists,
            0x03 => {
                let revision = bytes.get_u64_le();
                ExpectedRevision::Revision(revision)
            }

            _ => {
                tracing::warn!("unknown expected revision flag");
                return None;
            }
        };

        Some(Self::Append {
            ident,
            expected,
            events: bytes,
        })
    }
}

enum Response {
    WrongExpectedRevision {
        expected: ExpectedRevision,
        current: ExpectedRevision,
    },

    Error,

    Committed {
        start: u64,
        next: u64,
    },
}

impl Response {
    fn wrong_expected_revision(expected: ExpectedRevision, current: ExpectedRevision) -> Self {
        Self::WrongExpectedRevision { expected, current }
    }

    fn error() -> Self {
        Self::Error
    }

    fn committed(start: u64, next: u64) -> Self {
        Self::Committed { start, next }
    }

    fn serialize(self, bytes: &mut BytesMut) -> Bytes {
        todo!()
    }
}
