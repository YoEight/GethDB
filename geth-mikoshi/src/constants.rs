pub const CHUNK_HEADER_SIZE: usize = 128;
pub const CHUNK_FOOTER_SIZE: usize = 128;
pub const CHUNK_SIZE: usize = 256 * 1024 * 1024;
pub const _CHUNK_FILE_SIZE: usize =
    _aligned_size(CHUNK_SIZE + CHUNK_HEADER_SIZE + CHUNK_FOOTER_SIZE);

const fn _aligned_size(size: usize) -> usize {
    if size % 4_096 == 0 {
        return size;
    }

    (size / 4_096 + 1) * 4_096
}
