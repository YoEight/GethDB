use crate::index::rannoch::block::{Block, Builder};
use bytes::BytesMut;

const NUM_OF_KEYS: usize = 100;

fn key_of(idx: usize) -> u64 {
    idx as u64 * 5
}

fn revision_of(idx: usize) -> u64 {
    idx as u64 * 42
}

fn position_of(idx: usize) -> u64 {
    idx as u64
}

fn generate_block() -> Block {
    let mut buffer = BytesMut::new();
    let mut builder = Block::builder(&mut buffer, 10_000);

    for idx in 0..NUM_OF_KEYS {
        let key = key_of(idx);
        let revision = revision_of(idx);
        let position = position_of(idx);

        assert!(builder.add(key, revision, position));
    }

    builder.build()
}

#[test]
fn test_block_build_single_key() {
    let mut buffer = BytesMut::new();
    let mut builder = Block::builder(&mut buffer, 26);

    assert!(builder.add(233, 2333, 23333));

    let block = builder.build();
    assert_eq!(1, block.len());

    let entry = block.read_entry(0);
    assert!(entry.is_some());

    let entry = entry.unwrap();
    assert_eq!(233, entry.key);
    assert_eq!(2333, entry.revision);
    assert_eq!(23333, entry.position);
}

#[test]
fn test_block_build_full() {
    let mut buffer = BytesMut::new();
    let mut builder = Block::builder(&mut buffer, 26);

    assert!(builder.add(1, 1, 1));
    assert!(!builder.add(2, 2, 2));

    let block = builder.build();
    assert_eq!(1, block.len());
}

#[test]
fn test_block_build_all() {
    let block = generate_block();

    for idx in 0..NUM_OF_KEYS {
        let entry = block.read_entry(idx).unwrap();

        assert_eq!(key_of(idx), entry.key);
        assert_eq!(revision_of(idx), entry.revision);
        assert_eq!(position_of(idx), entry.position);
    }
}

#[test]
fn test_block_encoding() {
    let mut buffer = BytesMut::new();
    let block = generate_block();
    let encoded = block.encode(&mut buffer);
    let decoded_block = Block::decode(encoded);

    for idx in 0..NUM_OF_KEYS {
        let entry_1 = block.read_entry(idx).unwrap();
        let entry_2 = decoded_block.read_entry(idx).unwrap();

        assert_eq!(entry_1.key, entry_2.key);
        assert_eq!(entry_1.revision, entry_2.revision);
        assert_eq!(entry_1.position, entry_2.position);
    }
}

#[test]
fn test_block_find_key() {
    let block = generate_block();
    for i in 0..NUM_OF_KEYS {
        let entry = block
            .find_entry(key_of(i), revision_of(i))
            .expect("entry to be defined");

        assert_eq!(
            entry.key,
            key_of(i),
            "expected key: {:?}, actual key: {:?}",
            key_of(i),
            entry.key
        );
        assert_eq!(
            entry.revision,
            revision_of(i),
            "expected revision: {:?}, actual revision: {:?}",
            revision_of(i),
            entry.revision,
        );
        assert_eq!(
            entry.position,
            position_of(i),
            "expected position: {:?}, actual position: {:?}",
            position_of(i),
            entry.position,
        );
    }
}
