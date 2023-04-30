use crate::index::{IteratorIO, IteratorIOExt};

mod ss_table;

pub struct TestValues {
    values: std::vec::IntoIter<(u64, u64, u64)>,
}

pub fn values(vs: &'static [(u64, u64, u64)]) -> impl IteratorIO<Item = (u64, u64, u64)> {
    vs.into_iter().copied().lift()
}
