use std::cmp::Ordering;
use std::collections::Bound;
use std::ops::RangeBounds;

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

#[derive(Debug, Copy, Clone)]
pub enum Range {
    Inbound,
    Outbound(Ordering),
}

pub fn range_start<R>(range: R) -> u64
where
    R: RangeBounds<u64>,
{
    match range.start_bound() {
        Bound::Included(start) => *start,
        Bound::Excluded(start) => *start + 1,
        Bound::Unbounded => 0,
    }
}

pub fn in_range<R>(range: &R, value: u64) -> Range
where
    R: RangeBounds<u64>,
{
    match (range.start_bound(), range.end_bound()) {
        (Bound::Included(start), Bound::Included(end)) => {
            if value < *start {
                return Range::Outbound(Ordering::Less);
            }

            if value > *end {
                return Range::Outbound(Ordering::Greater);
            }

            return Range::Inbound;
        }

        (Bound::Included(start), Bound::Excluded(end)) => {
            if value < *start {
                return Range::Outbound(Ordering::Less);
            }

            if value > *end - 1 {
                return Range::Outbound(Ordering::Greater);
            }

            return Range::Inbound;
        }

        (Bound::Included(start), Bound::Unbounded) => {
            if value < *start {
                return Range::Outbound(Ordering::Less);
            }

            return Range::Inbound;
        }

        (Bound::Excluded(start), Bound::Excluded(end)) => {
            if value < *start + 1 {
                return Range::Outbound(Ordering::Less);
            }

            if value > *end - 1 {
                return Range::Outbound(Ordering::Greater);
            }

            return Range::Inbound;
        }

        (Bound::Excluded(start), Bound::Included(end)) => {
            if value < *start + 1 {
                return Range::Outbound(Ordering::Less);
            }

            if value > *end {
                return Range::Outbound(Ordering::Greater);
            }

            return Range::Inbound;
        }

        (Bound::Excluded(start), Bound::Unbounded) => {
            if value < *start + 1 {
                return Range::Outbound(Ordering::Less);
            }

            return Range::Inbound;
        }

        (Bound::Unbounded, Bound::Included(end)) => {
            if value <= *end {
                return Range::Inbound;
            }

            Range::Outbound(Ordering::Greater)
        }

        (Bound::Unbounded, Bound::Excluded(end)) => {
            if value <= *end - 1 {
                return Range::Inbound;
            }

            Range::Outbound(Ordering::Greater)
        }

        (Bound::Unbounded, Bound::Unbounded) => Range::Inbound,
    }
}
