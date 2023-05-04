use bytes::Buf;
use digest::Digest;
use sha2::{digest, Sha512};

pub fn mikoshi_hash(value: &str) -> u64 {
    let mut hasher = Sha512::new();
    hasher.update(value);

    hasher.finalize().as_slice().get_u64_le()
}
