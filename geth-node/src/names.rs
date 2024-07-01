#![allow(dead_code)]
pub mod streams {
    pub static ALL: &'static str = "$all";
    pub static GLOBALS: &'static str = "$globals";
    pub static SYSTEM: &'static str = "$system";
}

pub mod types {
    pub static STREAM_DELETED: &'static str = "$stream-deleted";
    pub static EVENTS_WRITTEN: &'static str = "$events-written";
}
