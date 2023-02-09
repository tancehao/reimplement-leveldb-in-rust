use crate::compare::ComparatorImpl;
use crate::memtable::MemTable;
use bytes::Bytes;

#[allow(unused)]
struct SkipList {}

impl MemTable for SkipList {
    fn empty() -> Self {
        todo!()
    }

    fn approximate_size(&self) -> u64 {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn get(&self, _key: &[u8]) -> Option<Option<Bytes>> {
        todo!()
    }

    fn set(&mut self, _key: Bytes, _seq_num: u64, _value: Bytes) {
        todo!()
    }

    fn del(&mut self, _key: Bytes, _seq_num: u64) {
        todo!()
    }

    fn last_seq_num(&self) -> u64 {
        todo!()
    }

    fn iter(&self, _c: ComparatorImpl) -> Box<dyn Iterator<Item = (Bytes, u64, Option<Bytes>)>> {
        todo!()
    }
}
