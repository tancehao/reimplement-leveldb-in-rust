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

const STATUS_WAL_BUFFERED: u8 = 1;
const STATUS_WAL_WRITTEN: u8 = 2;

impl Batch {
    pub fn set_seq_num(&mut self, seq: u64) {
        self.seq_num = seq;
    }

    pub fn get_seq_num(&self) -> u64 {
        self.seq_num & ((1<<56) - 1)
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

    pub fn set_wal_buffered(&mut self) {
        self.seq_num = self.seq_num | ((STATUS_WAL_BUFFERED as u64) << 56);
    }

    pub fn get_wal_buffered(&self) -> bool {
        ((self.seq_num >> 56) as u8 & STATUS_WAL_BUFFERED) > 0
    }

    pub fn set_wal_written(&mut self) {
        self.seq_num = self.seq_num | ((STATUS_WAL_WRITTEN as u64) << 56);
    }

    pub fn get_wal_written(&self) -> bool {
        ((self.seq_num >> 56) as u8 & STATUS_WAL_WRITTEN) > 0
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
                Some((self.batch.get_seq_num() + self.current, k, v.as_ref()))
            }
        }
    }
}

impl Encoding for Batch {
    fn encode(&self, dst: &mut BytesMut, _opts: &Opts) -> usize {
        let s = dst.len();
        put_uvarint(dst, self.get_seq_num());
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

    fn decode(src: &mut Bytes, _opts: &Opts) -> Result<Self, LError> {
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

#[cfg(test)]
mod test {
    use bytes::BytesMut;
    use crate::batch::Batch;
    use crate::io::Encoding;
    use crate::opts::Opts;

    #[test]
    fn test_batch() {
        let opt = Opts::default();
        let mut batch = Batch::default();
        batch.set("k1".into(), "v1".into());
        batch.delete("k2".into());
        batch.set_seq_num(100);
        assert_eq!(batch.get_seq_num(), 100);
        assert_eq!(batch.cmds().len(), 2);
        let mut bm = BytesMut::new();
        batch.encode(&mut bm, &opt);
        let mut b = bm.freeze();
        let batch2 = Batch::decode(&mut b, &opt).unwrap();
        assert_eq!(batch.get_seq_num(), batch2.get_seq_num());
        let (mut i1, mut i2) = (batch.iter(), batch2.iter());
        assert_eq!(i1.next(), i2.next());
        assert_eq!(i1.next(), i2.next());
        assert_eq!(i1.next(), None);
        assert_eq!(i2.next(), None);

        assert!(!batch.get_wal_buffered());
        batch.set_wal_buffered();
        assert!(batch.get_wal_buffered());
        assert_eq!(batch.get_seq_num(), 100);

        assert!(!batch.get_wal_written());
        batch.set_wal_written();
        assert!(batch.get_wal_written());
        assert_eq!(batch.get_seq_num(), 100);

        let mut bm = BytesMut::new();
        batch.encode(&mut bm, &opt);
        let mut b = bm.freeze();
        let batch3 = Batch::decode(&mut b, &opt).unwrap();
        assert_eq!(batch.get_seq_num(), batch3.get_seq_num());
        assert!(!batch3.get_wal_written());
        let (mut i1, mut i3) = (batch.iter(), batch3.iter());
        assert_eq!(i1.next(), i3.next());
        assert_eq!(i1.next(), i3.next());
        assert_eq!(i1.next(), None);
        assert_eq!(i3.next(), None);
    }
}