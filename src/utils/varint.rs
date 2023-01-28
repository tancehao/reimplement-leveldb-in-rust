use crate::LError;

use bytes::{BufMut, Bytes, BytesMut};

const MAX_VARINT_LEN_64: usize = 10;

pub fn varint(buf: &[u8]) -> Option<(i64, usize)> {
    uvarint(buf).map(|(ux, n)| {
        let mut x = ux >> 1;
        if ux & 1 != 0 {
            x = x ^ u64::MAX;
        }
        (x as i64, n)
    })
}

pub fn uvarint(buf: &[u8]) -> Option<(u64, usize)> {
    let (mut x, mut s) = (0u64, 0usize);
    for i in 0..buf.len() {
        if i == MAX_VARINT_LEN_64 {
            return None;
        }
        let c = buf[i];
        if c < 0x80 {
            if i == MAX_VARINT_LEN_64 - 1 && c > 1 {
                return None;
            }
            return Some((x | ((c as u64) << s), i + 1));
        }
        x |= ((c & 0x7f) as u64) << s;
        s += 7;
    }
    None
}

pub fn take_varint(buf: &mut Bytes) -> Option<i64> {
    take_uvarint(buf).map(|ux| {
        let mut x = ux >> 1;
        if ux & 1 != 0 {
            x = x ^ u64::MAX;
        }
        x as i64
    })
}

pub fn take_uvarint(buf: &mut Bytes) -> Option<u64> {
    let (mut x, mut s) = (0u64, 0usize);
    for i in 0..buf.len() {
        if i == MAX_VARINT_LEN_64 {
            return None;
        }
        let c = buf[i];
        if c < 0x80 {
            if i == MAX_VARINT_LEN_64 - 1 && c > 1 {
                return None;
            }
            let _ = buf.split_to(i + 1);
            return Some(x | ((c as u64) << s));
        }
        x |= ((c & 0x7f) as u64) << s;
        s += 7;
    }
    None
}

pub fn must_take_uvarint(but: &mut Bytes, err: &LError) -> Result<u64, LError> {
    take_uvarint(but).ok_or(err.clone())
}

pub fn put_varint(buf: &mut BytesMut, x: i64) -> usize {
    let mut ux = (x as u64) << 1;
    if x < 0 {
        ux ^= u64::MAX;
    }
    return put_uvarint(buf, ux);
}

pub fn put_uvarint(buf: &mut BytesMut, mut x: u64) -> usize {
    let mut i = 0;
    while x >= 0x80 {
        buf.put_u8((x as u8) | 0x80);
        x >>= 7;
        i = i + 1;
    }
    buf.put_u8(x as u8);
    i + 1
}

#[cfg(test)]
mod tests {
    use crate::utils::varint::{put_varint, varint};
    use bytes::BytesMut;

    #[test]
    fn test_varint() {
        let mut buf = BytesMut::new();
        let cases = vec![
            1i64,
            -1,
            1 << 9,
            -1 << 9,
            1 << 17,
            -1 << 17,
            1 << 33,
            -1 << 33,
            1 << 62,
            -1 << 62,
            1 << 63 - 1,
            -1 << 63,
        ];
        // let cases = vec![-1i64<<63];
        let mut sizes = vec![];
        for c in cases.iter() {
            let s = put_varint(&mut buf, *c);
            sizes.push(s);
        }
        let mut pos = 0;
        for i in 0..sizes.len() {
            let ss = varint(buf[pos..].as_ref());
            assert_eq!(ss, Some((cases[i], sizes[i])));
            pos += ss.unwrap().1;
        }
    }
}
