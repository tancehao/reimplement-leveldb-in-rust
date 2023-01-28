use crate::compare::Comparator;
use bytes::Bytes;
pub mod simple;
pub mod skiplist;

pub trait MemTable: Sync + Send + Sized + 'static {
    fn empty() -> Self;

    fn approximate_size(&self) -> u64;

    fn len(&self) -> usize;

    // get the latest value for key. no MVCC currently.
    fn get(&self, key: &[u8]) -> Option<Bytes>;

    fn set(&mut self, key: Bytes, seq_num: u64, value: Bytes);

    fn del(&mut self, key: Bytes, seq_num: u64);

    fn iter<C: Comparator>(&self, c: C) -> Box<dyn Iterator<Item = (Bytes, u64, Option<Bytes>)>>;
}
