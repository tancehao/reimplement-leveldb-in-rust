use linked_hash_map_rs::LinkedHashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

pub(crate) trait CacheSize {
    fn appropriate_size(&self) -> usize;
}

pub(crate) struct LRUCache<K: CacheSize + Hash + Eq, V: CacheSize> {
    cache: LinkedHashMap<K, V>,
    size: usize,
    size_limit: usize,
}

impl<K: CacheSize + Hash + Eq, V: CacheSize> LRUCache<K, V> {
    pub fn new(size_limit: usize) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            cache: LinkedHashMap::new(),
            size_limit,
            size: 0,
        }))
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn limit(&self) -> usize {
        self.size_limit
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn get(&mut self, k: &K) -> Option<&V> {
        self.cache.move_to_back(k).map(|(_, db)| db)
    }

    pub fn set(&mut self, key: K, val: V) {
        let size = match self.cache.get(&key) {
            None => 0,
            Some(v) => {
                let s = v.appropriate_size();
                self.cache.remove(&key);
                s + key.appropriate_size()
            }
        };
        self.size = self.size - size + key.appropriate_size() + val.appropriate_size();
        self.cache.push_back(key, val);
        while self.size > self.size_limit {
            match self.cache.pop_front() {
                None => break,
                Some((k, v)) => {
                    self.size -= k.appropriate_size() + v.appropriate_size();
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::utils::lru::{CacheSize, LRUCache};

    #[test]
    fn test_lru() {
        struct Ele {}

        impl CacheSize for Ele {
            fn appropriate_size(&self) -> usize {
                1
            }
        }

        impl CacheSize for u32 {
            fn appropriate_size(&self) -> usize {
                4
            }
        }

        impl CacheSize for u8 {
            fn appropriate_size(&self) -> usize {
                1
            }
        }

        let buf = LRUCache::new(14);
        let mut buf = buf.lock().unwrap();
        assert_eq!(buf.limit(), 14);
        buf.set(1u32, 11u8);
        assert_eq!(buf.size(), 5);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.get(&1), Some(&11));
        buf.set(2, 22);
        assert_eq!(buf.size(), 10);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.get(&2), Some(&22));
        buf.set(3, 33);
        assert_eq!(buf.size(), 10);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.get(&3), Some(&33));
        assert_eq!(buf.get(&1), None);
        assert_eq!(buf.get(&2), Some(&22));
        buf.set(4, 44);
        assert_eq!(buf.size(), 10);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.get(&4), Some(&44));
        assert_eq!(buf.get(&3), None);
        buf.set(2, 222);
        assert_eq!(buf.size(), 10);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.get(&4), Some(&44));
        assert_eq!(buf.get(&2), Some(&222));
        buf.set(5, 55);
        assert_eq!(buf.size(), 10);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.get(&5), Some(&55));
        assert_eq!(buf.get(&2), Some(&222));
        assert_eq!(buf.get(&4), None);
    }
}
