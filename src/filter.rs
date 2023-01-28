use bytes::{Bytes, BytesMut};
use std::cmp::{max, min};

pub static BLOOM_FILTER: BloomFilter = BloomFilter { bits_per_key: 10 };

pub trait FilterPolicy {
    fn name() -> &'static str;

    fn append_filter(&self, dst: &mut BytesMut, keys: &[Bytes]) -> BytesMut;

    fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool;
}

#[derive(Clone, Debug)]
pub struct BloomFilter {
    bits_per_key: usize,
}

impl FilterPolicy for BloomFilter {
    fn name() -> &'static str {
        "leveldb.BuiltinBloomFilter2"
    }

    fn append_filter(&self, dst: &mut BytesMut, keys: &[Bytes]) -> BytesMut {
        let bits_per_key = max(self.bits_per_key, 0);
        let k = min(max(((bits_per_key as f64) * 0.69) as u32, 1), 30);
        let mut n_bits = max(keys.len() * (bits_per_key as usize), 64);
        let n_bytes = (n_bits + 7) / 8;
        n_bits = n_bytes * 8;
        let (mut overall, mut filter) = extend(dst, n_bytes + 1);
        for key in keys {
            let mut h = hash(key.as_ref());
            let delta = (h >> 17) | (h << 15);
            for _ in 0..k {
                let bit_pos = (h % (n_bits as u32)) as usize;
                filter[bit_pos / 8] |= 1 << (bit_pos % 8);
                h = h.wrapping_add(delta);
            }
        }
        filter[n_bytes] = k as u8;
        overall.unsplit(filter);
        overall
    }

    fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool {
        if filter.len() < 2 {
            return false;
        }
        let k = filter[filter.len() - 1];
        if k > 30 {
            return true;
        }
        let n_bits = ((filter.len() - 1) * 8) as u32;
        let mut h = hash(key);
        let delta = (h >> 17) | (h << 15);
        for _ in 0..k {
            let bit_pos = (h % n_bits) as usize;
            if filter[bit_pos / 8] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
            h += delta;
        }
        true
    }
}

fn extend(d: &mut BytesMut, n: usize) -> (BytesMut, BytesMut) {
    let want = n + d.len();
    if want <= d.capacity() {
        let overall = d.split_off(want);
        d.iter_mut().for_each(|x| *x = 0);
        return (overall, d.clone());
    } else {
        let mut c: usize = 1024;
        while c < want {
            c += c / 4;
        }
        let mut overall = BytesMut::with_capacity(c);
        overall.resize(want, 0);
        let trailer = overall.split_off(d.len());
        return (overall, trailer);
    }
}

pub fn hash(data: &[u8]) -> u32 {
    // Similar to murmur hash
    let n = data.len();
    const M: u32 = 0xc6a4a793;
    const SEED: u32 = 0xbc9f1d34;
    let r = 24;
    let mut h = SEED ^ (M.wrapping_mul(n as u32));

    // Pick up four bytes at a time
    let mut i = 0;
    while i + 4 <= n {
        let w = u32::from_le_bytes(data[i..i + 4].as_ref().try_into().unwrap());
        i += 4;
        h = h.wrapping_add(w);
        h = h.wrapping_mul(M);
        h ^= h >> 16;
    }

    let diff = n - i;
    if diff >= 3 {
        h += (data[i + 2] as u32) << 16
    };
    if diff >= 2 {
        h += (data[i + 1] as u32) << 8
    };
    if diff >= 1 {
        h += data[i] as u32;
        h = h.wrapping_mul(M);
        h ^= h >> r;
    }

    h
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_small_bloom_filter() {
        // TODO
    }
}
