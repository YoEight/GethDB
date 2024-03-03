#[cfg(test)]
use arbitrary::{Arbitrary, Unstructured};
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

#[derive(Debug, Clone)]
pub struct Entry {
    pub index: u64,
    pub term: u64,
    pub payload: Bytes,
}

#[cfg(test)]
impl<'a> Arbitrary<'a> for Entry {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let size = u.arbitrary_len::<u8>()?;
        let mut payload = Vec::<u8>::with_capacity(size);

        for _ in 0..size {
            payload.push(u.arbitrary()?);
        }

        Ok(Entry {
            index: u.arbitrary()?,
            term: u.arbitrary()?,
            payload: payload.into(),
        })
    }
}
