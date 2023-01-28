use crate::compare::Comparator;
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

    fn get(&self, _key: &[u8]) -> Option<Bytes> {
        todo!()
    }

    fn set(&mut self, _key: Bytes, _seq_num: u64, _value: Bytes) {
        todo!()
    }

    fn del(&mut self, _key: Bytes, _seq_num: u64) {
        todo!()
    }

    fn iter<C: Comparator>(&self, _c: C) -> Box<dyn Iterator<Item = (Bytes, u64, Option<Bytes>)>> {
        todo!()
    }
}
