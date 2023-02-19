use crate::compare::ComparatorImpl;
use crate::memtable::MemTable;
use bytes::Bytes;
use std::cmp::max;
use std::collections::BTreeMap;

#[derive(Default, Debug)]
pub struct BTMap {
    size_hint: u64,
    data: BTreeMap<Bytes, (u64, Option<Bytes>)>,
    seq_num: u64,
}

impl MemTable for BTMap {
    fn empty() -> Self {
        BTMap {
            size_hint: 0,
            data: BTreeMap::new(),
            seq_num: 0,
        }
    }

    fn last_seq_num(&self) -> u64 {
        self.seq_num
    }

    fn approximate_size(&self) -> u64 {
        self.size_hint
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn get(&self, key: &[u8]) -> Option<Option<Bytes>> {
        self.data.get(key).map(|(_, x)| x.clone())
    }

    fn set(&mut self, key: Bytes, seq_num: u64, value: Bytes) {
        match self.data.get_mut(&key) {
            None => {
                self.size_hint += (key.len() + 8 + value.len()) as u64;
                self.data.insert(key.clone(), (seq_num, Some(value)));
                self.seq_num = max(seq_num, self.seq_num);
            }
            Some((seq, oldv)) => {
                if *seq < seq_num {
                    self.size_hint += value.len() as u64;
                    self.size_hint -= oldv.as_ref().map(|x| x.len()).unwrap_or_default() as u64;
                    *oldv = Some(value);
                    *seq = seq_num;
                    self.seq_num = seq_num;
                }
            }
        }
    }

    fn del(&mut self, key: Bytes, seq_num: u64) {
        match self.data.get_mut(&key) {
            None => {
                self.size_hint += (key.len() + 8) as u64;
                self.data.insert(key, (seq_num, None));
                self.seq_num = max(seq_num, self.seq_num);
            }
            Some((seq, oldv)) => {
                if *seq < seq_num {
                    self.size_hint -= oldv.as_ref().map(|x| x.len()).unwrap_or_default() as u64;
                    *oldv = None;
                    *seq = seq_num;
                    self.seq_num = seq_num;
                }
            }
        }
    }

    // a little inefficient here. maybe we can keep the btmap in expected order when inserting or updating.
    fn iter(&self, c: ComparatorImpl) -> Box<dyn Iterator<Item = (Bytes, u64, Option<Bytes>)>> {
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
    use crate::compare::BYTEWISE_COMPARATOR;
    use crate::memtable::simple::BTMap;
    use crate::memtable::MemTable;
    use bytes::Bytes;
    use std::collections::HashSet;

    #[test]
    fn test_btmap() {
        let mut m = BTMap::default();
        let mut ks = HashSet::new();
        for i in 1000u64..10000 {
            ks.insert(i);
        }

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
            if *n % 20 == 0 {
                m.del(k.clone(), *n);
            } else {
                m.set(k.clone(), *n, v.clone());
            }
        }
        assert_eq!(m.len(), 9000);
        for (k, n, v) in kvs.iter() {
            assert_eq!(
                m.get(k.as_ref()),
                if *n % 20 == 0 {
                    Some(None)
                } else {
                    Some(Some(v.clone()))
                }
            );
        }
        let mut i = m.iter(BYTEWISE_COMPARATOR);
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
