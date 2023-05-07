use crate::storage::InMemoryStorage;
use crate::wal::ChunkManager;
use geth_common::Propose;
use std::io;
use uuid::Uuid;

#[test]
fn test_in_mem_new_chunk() -> io::Result<()> {
    let storage = InMemoryStorage::new();
    let manager = ChunkManager::load(storage)?;

    let propose = Propose {
        id: Uuid::new_v4(),
        r#type: "baz".to_string(),
        data: serde_json::to_vec(&serde_json::json!({
            "rust": 42u64,
        }))
        .unwrap()
        .into(),
    };

    let (log_pos, next_log_pos) = manager.append("foobar".to_string(), 0, vec![propose.clone()])?;

    assert_eq!(log_pos, 0);
    assert!(log_pos < next_log_pos);

    let mut iter = manager.prepare_logs(log_pos);

    let prepare = iter.next()?.unwrap();

    assert_eq!(log_pos, prepare.logical_position);
    assert_eq!(propose.id, prepare.event_id);
    assert_eq!(propose.r#type, prepare.event_type);
    assert_eq!(propose.data, prepare.data);

    assert!(iter.next()?.is_none());

    Ok(())
}
