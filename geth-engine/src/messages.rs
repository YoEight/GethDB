use eyre::Report;

use crate::process::reading;

pub enum ReadStreamCompleted {
    StreamDeleted,
    Unexpected(Report),
    Success(reading::Streaming),
}

impl ReadStreamCompleted {
    pub fn success(self) -> eyre::Result<reading::Streaming> {
        match self {
            ReadStreamCompleted::StreamDeleted => eyre::bail!("stream deleted"),
            ReadStreamCompleted::Unexpected(e) => Err(e),
            ReadStreamCompleted::Success(streaming) => Ok(streaming),
        }
    }
}
