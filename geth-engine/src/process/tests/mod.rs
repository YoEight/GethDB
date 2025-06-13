use serde::{Deserialize, Serialize};

mod indexing;
mod interactions;
mod programs;
mod reading;
mod subscribing;
mod writing;

#[ctor::ctor]
fn test_init() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .init();
}

#[derive(Serialize, Deserialize)]
pub struct Foo {
    pub baz: u32,
}
