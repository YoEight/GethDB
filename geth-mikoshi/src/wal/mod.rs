mod chunk;
mod footer;
mod header;
mod manager;
mod parsing;
mod record;

#[derive(Debug)]
pub struct Proposition {
    pub stream_key: u64,
    pub transaction_position: u64,
    pub claims: Vec<u64>,
}

impl Proposition {
    pub fn new(stream_key: u64, transaction_position: u64) -> Self {
        Self {
            stream_key,
            transaction_position,
            claims: Vec::new(),
        }
    }
}
