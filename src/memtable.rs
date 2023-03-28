use crate::compare::ComparatorImpl;
use bytes::Bytes;

pub mod simple;
pub mod skiplist;

pub trait MemTable: Sync + Send + Sized + 'static {
    fn empty() -> Self;

    fn approximate_size(&self) -> u64;

    fn len(&self) -> usize;

    // get the latest value for key. no MVCC currently.
    fn get(&self, key: &[u8], snapshot: u64) -> Option<Option<Bytes>>;

    fn set(&mut self, key: Bytes, seq_num: u64, value: Option<Bytes>);

    fn last_seq_num(&self) -> u64;

    fn iter(&self, c: ComparatorImpl) -> Box<dyn Iterator<Item = (Bytes, u64, Option<Bytes>)>>;

    fn snapshot_acquired(&mut self, _snapshot: u64) {}

    fn snapshot_released(&mut self, _snapshot: u64) {}
}
