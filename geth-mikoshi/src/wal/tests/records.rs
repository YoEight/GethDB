use crate::wal::record::{PrepareFlags, PrepareLog};
use bytes::{Bytes, BytesMut};
use chrono::Utc;
use uuid::Uuid;

#[test]
fn test_prepare_log_serialization() {
    let mut buffer = BytesMut::new();
    let expected = PrepareLog {
        logical_position: 1234,
        flags: PrepareFlags::HAS_DATA | PrepareFlags::IS_JSON,
        transaction_position: 34,
        transaction_offset: 50,
        revision: 42,
        tenant_id: "toto".to_string(),
        event_stream_id: "foobar".to_string(),
        event_id: Uuid::new_v4(),
        correlation_id: Uuid::new_v4(),
        created: Utc::now().timestamp(),
        event_type: "baz".to_string(),
        data: Bytes::from(r#"this is important data"#),
        metadata: Default::default(),
    };

    expected.put(&mut buffer);
    let actual = PrepareLog::get(buffer.freeze());

    assert_eq!(expected.logical_position, actual.logical_position);
    assert_eq!(expected.flags, actual.flags);
    assert_eq!(expected.transaction_position, actual.transaction_position);
    assert_eq!(expected.transaction_offset, actual.transaction_offset);
    assert_eq!(expected.revision, actual.revision);
    assert_eq!(expected.event_stream_id, actual.event_stream_id);
    assert_eq!(expected.event_id, actual.event_id);
    assert_eq!(expected.correlation_id, actual.correlation_id);
    assert_eq!(expected.created, actual.created);
    assert_eq!(expected.event_type, actual.event_type);
    assert_eq!(expected.data, actual.data);
    assert_eq!(expected.metadata, actual.metadata);
}
