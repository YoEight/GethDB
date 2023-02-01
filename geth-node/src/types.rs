use geth_common::Position;
use uuid::Uuid;

pub struct WriteResult {
    pub correlation: Uuid,
    pub position: Position,
}
