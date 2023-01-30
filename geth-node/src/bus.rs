use crate::messages::{AppendStream, AppendStreamCompleted, ReadStream, ReadStreamCompleted};

#[derive(Clone)]
pub struct Bus {}

impl Bus {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn read_stream(&self, msg: ReadStream) -> ReadStreamCompleted {
        todo!()
    }

    pub async fn append_stream(&self, msg: AppendStream) -> AppendStreamCompleted {
        todo!()
    }
}
