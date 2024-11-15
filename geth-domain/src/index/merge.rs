use std::cmp::Ordering;
use std::io;

use geth_common::IteratorIO;

use crate::index::block::BlockEntry;
use crate::index::mem_table::NoMemTable;

use super::ss_table::NoSSTable;

pub struct MergeBuilder<TMemTable, TSSTable> {
    mem_tables: Vec<TMemTable>,
    ss_tables: Vec<TSSTable>,
}

impl<TMemTable, TSSTable> MergeBuilder<TMemTable, TSSTable> {
    pub fn build(self) -> Merge<TMemTable, TSSTable> {
        let len = self.mem_tables.len() + self.ss_tables.len();
        let mut caches = Vec::with_capacity(len);

        for _ in 0..len {
            caches.push(None);
        }

        Merge {
            mem_tables: self.mem_tables,
            ss_tables: self.ss_tables,
            caches,
        }
    }

    pub fn push_mem_table_scan(&mut self, mem_table_scan: TMemTable) {
        self.mem_tables.push(mem_table_scan);
    }

    pub fn push_ss_table_scan(&mut self, ss_table_scan: TSSTable) {
        self.ss_tables.push(ss_table_scan);
    }
}

pub struct Merge<TMemTable, TSSTable> {
    mem_tables: Vec<TMemTable>,
    ss_tables: Vec<TSSTable>,
    caches: Vec<Option<BlockEntry>>,
}

impl<TSSTable> Merge<NoMemTable, TSSTable> {
    pub fn builder_for_ss_tables_only() -> MergeBuilder<NoMemTable, TSSTable> {
        MergeBuilder {
            mem_tables: vec![],
            ss_tables: vec![],
        }
    }
}

impl<TMemTable> Merge<TMemTable, NoSSTable> {
    pub fn builder_for_mem_tables_only() -> MergeBuilder<TMemTable, NoSSTable> {
        MergeBuilder {
            mem_tables: vec![],
            ss_tables: vec![],
        }
    }
}

impl<TMemTable, TSSTable> Merge<TMemTable, TSSTable>
where
    TMemTable: Iterator<Item = BlockEntry>,
    TSSTable: IteratorIO<Item = BlockEntry>,
{
    pub fn builder() -> MergeBuilder<TMemTable, TSSTable> {
        MergeBuilder {
            mem_tables: vec![],
            ss_tables: vec![],
        }
    }

    fn pull_from_caches(&mut self) -> Option<BlockEntry> {
        let mut lower: Option<(usize, BlockEntry)> = None;

        for (idx, cell) in self.caches.iter_mut().enumerate() {
            if let Some(cell_value) = *cell {
                if let Some((entry_idx, entry)) = lower.as_mut() {
                    match entry.cmp_key_id(&cell_value) {
                        Ordering::Less => continue,
                        Ordering::Equal => *cell = None,
                        Ordering::Greater => {
                            *entry_idx = idx;
                            *entry = cell_value;
                        }
                    }
                } else {
                    lower = Some((idx, cell_value));
                }
            }
        }

        if let Some((idx, _value)) = lower {
            return self.caches[idx].take();
        }

        None
    }

    fn fill_caches(&mut self) -> io::Result<bool> {
        let mut found = false;

        for (index, cell) in self.caches.iter_mut().enumerate() {
            if cell.is_none() {
                let value = if index < self.mem_tables.len() {
                    Ok(self.mem_tables[index].next())
                } else {
                    self.ss_tables[index - self.mem_tables.len()].next()
                }?;

                found |= value.is_some();
                *cell = value;
            } else {
                found = true;
            }
        }

        Ok(found)
    }
}

impl<TMemTable, TSSTable> IteratorIO for Merge<TMemTable, TSSTable>
where
    TMemTable: Iterator<Item = BlockEntry>,
    TSSTable: IteratorIO<Item = BlockEntry>,
{
    type Item = BlockEntry;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        if self.fill_caches()? {
            return Ok(self.pull_from_caches());
        }

        Ok(None)
    }
}
