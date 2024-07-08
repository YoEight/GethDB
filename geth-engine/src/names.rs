#![allow(dead_code)]
pub mod streams {
    pub static ALL: &str = "$all";
    pub static GLOBALS: &str = "$globals";
    pub static SYSTEM: &str = "$system";
}

pub mod types {
    pub static STREAM_DELETED: &str = "$stream-deleted";
    pub static EVENTS_WRITTEN: &str = "$events-written";
    pub static EVENTS_INDEXED: &str = "$events-indexed";
}
