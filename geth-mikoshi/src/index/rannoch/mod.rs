pub(crate) mod block;
pub(crate) mod in_mem;
mod lsm;
mod mem_table;
mod merge;
mod ss_table;
#[cfg(test)]
mod tests;

#[derive(Copy, Clone)]
pub struct IndexedPosition {
    pub revision: u64,
    pub position: u64,
}
