use crate::compare::ComparatorImpl;
use crate::memtable::MemTable;
use bytes::Bytes;
use std::cmp::max;
use std::cmp::Ordering;
use std::collections::btree_map::BTreeMap;
use std::collections::btree_set::BTreeSet;
use std::collections::vec_deque::VecDeque;
use std::ops::Bound::{Excluded, Included};

#[derive(Default, Debug)]
pub struct BTMap {
    size_hint: u64,
    data: BTreeMap<Bytes, VecDeque<(u64, Option<Bytes>)>>,
    seq_num: u64,
    snapshots: BTreeSet<u64>,
}

impl MemTable for BTMap {
    fn empty() -> Self {
        BTMap {
            size_hint: 0,
            data: BTreeMap::new(),
            seq_num: 0,
            snapshots: BTreeSet::new(),
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

    fn get(&self, key: &[u8], snapshot: u64) -> Option<Option<Bytes>> {
        self.data.get(key).map(|x| {
            x.iter()
                .rev()
                .find(|(x, _)| *x <= snapshot)
                .and_then(|(_, x)| x.clone())
        })
    }

    fn set(&mut self, key: Bytes, seq_num: u64, value: Option<Bytes>) {
        assert!(seq_num > self.seq_num);
        if !self.data.contains_key(&key) {
            self.size_hint += key.len() as u64;
            self.data.insert(key.clone(), VecDeque::new());
        }
        let vals = self.data.get_mut(&key).unwrap();
        self.size_hint += value.as_ref().map(|x| x.len() as u64).unwrap_or_default() + 8;
        vals.push_back((seq_num, value));

        // deleted the old versions if they are not captured by any snapshots
        if vals.len() > 1 {
            let mut prev: Option<(u64, Option<Bytes>)> = None;
            let mut deduped_vals = VecDeque::new();
            while let Some((new_seq_num, new_val)) = vals.pop_front() {
                if let Some((old_seq_num, old_val)) = prev.replace((new_seq_num, new_val)) {
                    let captured = self
                        .snapshots
                        .range((Included(old_seq_num), Excluded(new_seq_num)))
                        .next()
                        .is_some();
                    if captured {
                        deduped_vals.push_back((old_seq_num, old_val));
                        continue;
                    }
                    self.size_hint -= 8 + old_val.map(|x| x.len() as u64).unwrap_or_default();
                }
            }
            if let Some((n, val)) = prev {
                self.size_hint += 8 + val.as_ref().map(|x| x.len() as u64).unwrap_or_default();
                deduped_vals.push_back((n, val));
            }
            *vals = deduped_vals;
        }
        self.seq_num = max(seq_num, self.seq_num);
    }

    // a little inefficient here. maybe we can keep the btmap in expected order when inserting or updating.
    fn iter(&self, c: ComparatorImpl) -> Box<dyn Iterator<Item = (Bytes, u64, Option<Bytes>)>> {
        let mut kvs = vec![];
        for (k, vals) in self.data.iter() {
            for (seq_num, val) in vals.iter() {
                kvs.push((k.clone(), *seq_num, val.clone()))
            }
        }

        kvs.sort_by(
            |(ka, sa, _), (kb, sb, _)| match c.compare(ka.as_ref(), kb.as_ref()) {
                Ordering::Equal => sb.cmp(sa),
                o => o,
            },
        );
        Box::new(BTMapIter {
            kvs: kvs.into_iter(),
        })
    }

    fn snapshot_acquired(&mut self, snapshot: u64) {
        self.snapshots.insert(snapshot);
    }

    fn snapshot_released(&mut self, snapshot: u64) {
        self.snapshots.remove(&snapshot);
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

    #[test]
    fn test_btmap() {
        let mut m = BTMap::default();
        let kvs: Vec<(Bytes, u64, Bytes)> = (1000u64..10000)
            .map(|x| {
                (
                    Bytes::from(format!("key:{}", x)),
                    x,
                    Bytes::from(format!("value:{}", x)),
                )
            })
            .collect();

        for (k, n, v) in kvs.iter() {
            if *n % 20 == 0 {
                m.set(k.clone(), *n, None);
            } else {
                m.set(k.clone(), *n, Some(v.clone()));
            }
        }
        assert_eq!(m.len(), 9000);
        for (k, n, v) in kvs.iter() {
            assert_eq!(
                m.get(k.as_ref(), 10000),
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

    #[test]
    fn test_btmap_mvcc() {
        let mut m = BTMap::default();
        m.set("k1".into(), 10, Some("v1".into()));
        m.snapshot_acquired(11);
        m.set("k1".into(), 20, Some("v2".into()));

        let version1 = m.get("k1".as_bytes(), 11);
        assert_eq!(version1, Some(Some("v1".into())));
        m.snapshot_released(11);
        m.set("k1".into(), 30, Some("v3".into()));

        let version2 = m.get("k1".as_bytes(), 25);
        assert_eq!(version2, Some(None));
        let version3 = m.get("k1".as_bytes(), 35);
        assert_eq!(version3, Some(Some("v3".into())));
    }
}
