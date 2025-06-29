use serde::{Deserialize, Serialize};

mod indexing;
mod interactions;
mod programs;
mod reading;
mod subscribing;
mod writing;

#[derive(Serialize, Deserialize)]
pub struct Foo {
    pub baz: u32,
}
