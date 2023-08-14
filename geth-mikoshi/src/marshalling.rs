use bytes::{Buf, BufMut, Bytes, BytesMut};

pub fn variable_string_length_bytes_size(value: usize) -> usize {
    let mut value = value as u64;
    let mut count = 0usize;

    while value > 0x7F {
        count += 1;
        value >>= 7;
    }

    count + 1
}

pub fn put_string(src: &String, buf: &mut BytesMut) {
    let mut value = src.len() as u64;

    while value > 0x7F {
        buf.put_u8((value | 0x80) as u8);
        value >>= 7;
    }

    buf.put_u8(value as u8);
    buf.extend_from_slice(src.as_bytes());
}

pub fn get_string(buf: &mut Bytes) -> String {
    let mut string_len = 0u32;
    let mut shift = 0u8;

    loop {
        let current = buf.get_u8();

        string_len |= ((current & 0x7F) as u32) << shift;

        if (current & 0x80) == 0 {
            break;
        }

        shift += 7;

        if shift > (4 * 7) {
            panic!("Maximum encoding length exceeded");
        }
    }

    String::from_utf8(buf.copy_to_bytes(string_len as usize).to_vec()).unwrap()
}
