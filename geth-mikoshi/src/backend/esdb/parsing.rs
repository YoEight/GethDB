use bytes::{Buf, BufMut};
use std::io;

#[cfg(test)]
mod tests {
    use crate::parsing::{read_string, write_string};
    use std::io::{self, Cursor, Read, Write};

    #[test]
    fn string_iso_ser_de() -> io::Result<()> {
        use bytes::BufMut;
        let mut buffer = bytes::BytesMut::new();
        let expected = "This is a string".to_string();

        write_string(&expected, &mut buffer);

        let actual = read_string(buffer.split().freeze())?;

        assert_eq!(expected, actual);

        Ok(())
    }
}

pub fn write_string(src: &String, buf: &mut bytes::BytesMut) {
    let mut value = src.len() as u64;

    while value > 0x7F {
        buf.put_u8((value | 0x80) as u8);
        value >>= 7;
    }

    buf.put_u8(value as u8);
    buf.extend_from_slice(src.as_bytes());
}

pub fn read_string<R>(mut buf: R) -> io::Result<String>
where
    R: Buf,
{
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Maximum encoding length exceeded",
            ));
        }
    }

    String::from_utf8(buf.copy_to_bytes(string_len as usize).to_vec()).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Invalid UTF-8 string: {}", e),
        )
    })
}

pub fn read_uuid<R>(buf: &mut R) -> io::Result<uuid::Uuid>
where
    R: Buf,
{
    if let Some(uuid) = uuid::Uuid::from_slice(buf.copy_to_bytes(16).as_ref()).ok() {
        return Ok(uuid);
    }

    Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid uuid"))
}
