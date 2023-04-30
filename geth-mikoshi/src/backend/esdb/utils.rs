









pub fn chunk_filename_from(seq_number: usize, version: usize) -> String {
    format!("chunk-{:06}.{:06}", seq_number, version)
}

pub fn variable_string_length_bytes_size(value: usize) -> usize {
    let mut value = value as u64;
    let mut count = 0usize;

    while value > 0x7F {
        count += 1;
        value >>= 7;
    }

    count + 1
}
