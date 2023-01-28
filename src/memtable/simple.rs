use crate::compare::Comparator;
use crate::memtable::MemTable;
use bytes::Bytes;
use std::collections::BTreeMap;

#[derive(Default, Debug)]
pub struct BTMap {
    size_hint: u64,
    data: BTreeMap<Bytes, (u64, Option<Bytes>)>,
}

impl MemTable for BTMap {
    fn empty() -> Self {
        BTMap {
            size_hint: 0,
            data: BTreeMap::new(),
        }
    }

    fn approximate_size(&self) -> u64 {
        self.size_hint
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn get(&self, key: &[u8]) -> Option<Bytes> {
        match self.data.get(key) {
            None => None,
            Some((_, v)) => v.clone(),
        }
    }

    fn set(&mut self, key: Bytes, seq_num: u64, value: Bytes) {
        match self.data.get_mut(&key) {
            None => {
                self.size_hint += (key.len() + 8 + value.len()) as u64;
                self.data.insert(key, (seq_num, Some(value)));
            }
            Some((seq, oldv)) => {
                if *seq < seq_num {
                    self.size_hint += value.len() as u64;
                    self.size_hint -= oldv.as_ref().map(|x| x.len()).unwrap_or_default() as u64;
                    *oldv = Some(value);
                }
            }
        }
    }

    fn del(&mut self, key: Bytes, seq_num: u64) {
        match self.data.get_mut(&key) {
            None => {
                self.size_hint += (key.len() + 8) as u64;
                self.data.insert(key, (seq_num, None));
            }
            Some((seq, oldv)) => {
                if *seq < seq_num {
                    self.size_hint -= oldv.as_ref().map(|x| x.len()).unwrap_or_default() as u64;
                    *oldv = None;
                }
            }
        }
    }

    // a litter inefficient here. maybe we can keep the btmap in expected order when inserting or updating.
    fn iter<C: Comparator>(&self, c: C) -> Box<dyn Iterator<Item = (Bytes, u64, Option<Bytes>)>> {
        let mut kvs: Vec<(Bytes, u64, Option<Bytes>)> = self
            .data
            .iter()
            .map(|(k, (n, v))| (k.clone(), *n, v.clone()))
            .collect();
        kvs.sort_by(|(ka, _, _), (kb, _, _)| c.compare(ka.as_ref(), kb.as_ref()));
        Box::new(BTMapIter {
            kvs: kvs.into_iter(),
        })
    }
}

pub(crate) struct BTMapIter {
    kvs: std::vec::IntoIter<(Bytes, u64, Option<Bytes>)>,
}

impl Iterator for BTMapIter {
    type Item = (Bytes, u64, Option<Bytes>);

    fn next(&mut self) -> Option<Self::Item> {
        self.kvs.next()
    }
}

#[cfg(test)]
mod test {
    use crate::compare::BytewiseComparator;
    use crate::memtable::simple::BTMap;
    use crate::memtable::MemTable;
    use bytes::Bytes;
    use std::collections::HashSet;

    #[test]
    fn test_btmap() {
        let mut m = BTMap::default();
        let mut ks = HashSet::new();
        for i in (1000u64..10000) {
            ks.insert(i);
        }
        // unordered
        let kvs: Vec<(Bytes, u64, Bytes)> = ks
            .iter()
            .map(|x| {
                (
                    Bytes::from(format!("key:{}", *x)),
                    *x,
                    Bytes::from(format!("value:{}", *x)),
                )
            })
            .collect();

        for (k, n, v) in kvs.iter() {
            m.set(k.clone(), *n, v.clone());
            if *n % 20 == 0 {
                m.del(k.clone(), *n + 10000);
            }
        }
        assert_eq!(m.len(), 9000);
        for (k, n, v) in kvs.iter() {
            assert_eq!(
                m.get(k.as_ref()),
                if *n % 20 == 0 { None } else { Some(v.clone()) }
            );
        }
        let c = BytewiseComparator::default();
        let mut i = m.iter(c);
        let mut prev = 0;
        let mut cnt = 0;
        while let Some((_, n, _)) = i.next() {
            assert!(n > prev);
            prev = n;
            cnt += 1;
        }
        assert_eq!(cnt, m.len());
    }
}
