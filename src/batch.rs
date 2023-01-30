use crate::compare::Comparator;
use crate::io::Encoding;
use crate::opts::Opts;
use crate::utils::varint::{put_uvarint, take_uvarint};
use crate::LError;
use bytes::{BufMut, Bytes, BytesMut};

#[derive(Debug, Clone, Default)]
pub struct Batch {
    seq_num: u64,
    kvs: Vec<(Bytes, Option<Bytes>)>,
}

impl Batch {
    pub fn set_seq_num(&mut self, seq: u64) {
        self.seq_num = seq;
    }

    pub fn count(&self) -> usize {
        self.kvs.len()
    }

    pub fn cmds(&self) -> &Vec<(Bytes, Option<Bytes>)> {
        self.kvs.as_ref()
    }

    pub fn set(&mut self, k: Bytes, v: Bytes) {
        self.kvs.push((k, Some(v)));
    }

    pub fn delete(&mut self, k: Bytes) {
        self.kvs.push((k, None));
    }

    pub fn iter(&self) -> BatchIter {
        BatchIter {
            batch: self,
            current: 0,
        }
    }
}

pub struct BatchIter<'a> {
    batch: &'a Batch,
    current: u64,
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = (u64, &'a Bytes, Option<&'a Bytes>);

    fn next(&mut self) -> Option<Self::Item> {
        match self.batch.kvs.get(self.current as usize) {
            None => None,
            Some((k, v)) => {
                self.current += 1;
                Some((self.batch.seq_num + self.current, k, v.as_ref()))
            }
        }
    }
}

impl Encoding for Batch {
    fn encode<T: Comparator>(&self, dst: &mut BytesMut, _opts: &Opts<T>) -> usize {
        let s = dst.len();
        put_uvarint(dst, self.seq_num);
        put_uvarint(dst, self.kvs.len() as u64);
        self.kvs.iter().for_each(|(k, v)| match v {
            None => {
                dst.put_u8(0);
                put_uvarint(dst, k.len() as u64);
                dst.extend_from_slice(k.as_ref());
            }
            Some(v) => {
                dst.put_u8(1);
                put_uvarint(dst, k.len() as u64);
                dst.extend_from_slice(k.as_ref());
                put_uvarint(dst, v.len() as u64);
                dst.extend_from_slice(v.as_ref());
            }
        });
        dst.len() - s
    }

    fn decode<T: Comparator>(src: &mut Bytes, _opts: &Opts<T>) -> Result<Self, LError> {
        let must_take_uvarint = |src: &mut Bytes| -> Result<u64, LError> {
            take_uvarint(src).ok_or(LError::InvalidFile(
                "the WAL file is malformat for uvarint is invalid".into(),
            ))
        };
        let seq_num = must_take_uvarint(src)?;
        let cnt = must_take_uvarint(src)?;
        let mut kvs = vec![];
        for _ in 0..cnt {
            let kind = u8::from_le_bytes(src.split_to(1).as_ref().try_into().unwrap());
            let key_len = must_take_uvarint(src)?;
            let key = src.split_to(key_len as usize);
            let value = match kind {
                0 => None,
                1 => {
                    let value_len = must_take_uvarint(src)?;
                    Some(src.split_to(value_len as usize))
                }
                _ => {
                    return Err(LError::InvalidFile(
                        "the WAL file is malformat for invalid key kind".into(),
                    ))
                }
            };
            kvs.push((key, value));
        }
        Ok(Batch { seq_num, kvs })
    }
}
