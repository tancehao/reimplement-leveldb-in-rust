use crate::compare::Comparator;
use crate::filter::{BloomFilter, BLOOM_FILTER};
use crate::io::{Encoding, Storage};
use crate::key::{InternalKey, InternalKeyRef};
use crate::opts::Opts;
use crate::utils::crc::crc32;
use crate::utils::varint::{put_uvarint, take_uvarint};
use crate::LError;
use bytes::{BufMut, Bytes, BytesMut};
use once_cell::sync::OnceCell;
use snap::raw::{Decoder, Encoder};
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub(crate) const FOOTER_SIZE: usize = 48;
const BLOCK_TRAILER_LEN: usize = 5;
const NO_COMPRESSION_BLOCK_TYPE: u8 = 0;
const SNAPPY_COMPRESSION_BLOCK_TYPE: u8 = 1;
const MAGIC_NUMBER: u64 = 0xdb4775248b80fb57;

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Default)]
pub struct BlockHandle {
    offset: u64,
    length: u64,
}

impl BlockHandle {
    #[inline]
    pub fn new(offset: u64, length: u64) -> Self {
        Self { offset, length }
    }
}

impl Encoding for BlockHandle {
    fn encode<C: Comparator>(&self, dst: &mut BytesMut, _opts: &Opts<C>) -> usize {
        let s0 = dst.len();
        put_uvarint(dst, self.offset);
        put_uvarint(dst, self.length);
        let s1 = dst.len();
        if s1 - s0 < 20 {
            dst.put_bytes(0, 20 - (s1 - s0));
        }
        20
    }

    fn decode<C: Comparator>(src: &mut Bytes, _opts: &Opts<C>) -> Result<Self, LError> {
        let mut buf = src.split_to(20);
        if let Some(offset) = take_uvarint(&mut buf) {
            if let Some(length) = take_uvarint(&mut buf) {
                return Ok(BlockHandle { offset, length });
            }
        }
        Err(LError::InvalidFile(format!(
            "invalid blockhandle: {:?}",
            src
        )))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Footer {
    magic: u64,
    pub(crate) index_handle: BlockHandle,
    pub(crate) metaindex_handle: BlockHandle,
}

impl Footer {
    pub fn new(ih: BlockHandle, mh: BlockHandle) -> Self {
        Self {
            magic: MAGIC_NUMBER,
            index_handle: ih,
            metaindex_handle: mh,
        }
    }
}

impl Encoding for Footer {
    fn encode<C: Comparator>(&self, dst: &mut BytesMut, opts: &Opts<C>) -> usize {
        self.metaindex_handle.encode(dst, opts);
        self.index_handle.encode(dst, opts);
        dst.put_u64_le(self.magic);
        48
    }

    fn decode<C: Comparator>(src: &mut Bytes, opts: &Opts<C>) -> Result<Self, LError> {
        if src.len() != FOOTER_SIZE {
            return Err(LError::InvalidFile(format!(
                "invalid footer: the size should be 48 bytes"
            )));
        }
        if src[FOOTER_SIZE - 8..].as_ref() != MAGIC_NUMBER.to_le_bytes() {
            return Err(LError::InvalidFile(format!("invalid magic number")));
        }
        let _ = src.split_off(FOOTER_SIZE - 8);
        let metaindex_handle = BlockHandle::decode(src, opts)?;
        let index_handle = BlockHandle::decode(src, opts)?;
        Ok(Self {
            magic: MAGIC_NUMBER,
            index_handle,
            metaindex_handle,
        })
    }
}

pub(crate) fn read_filter_data<C: Comparator, S: Storage>(
    file: &mut S,
    mut filter_meta_block: Bytes,
    opt: &Opts<C>,
) -> Result<Option<Filter>, LError> {
    let filter_name = match &opt.filter_name {
        None => return Ok(None),
        Some(name) => format!("filter.{}", name),
    };
    let v = &BLOOM_FILTER;
    let DecodedBlock { entries, .. } = IndexBlock::decode(&mut filter_meta_block, opt)?;
    for entry_group in entries.iter() {
        for i in 0..entry_group.len() {
            if let Some(key) = entry_group.get_nth_key(i) {
                if key.as_ref() == filter_name.as_bytes() {
                    let bh = entry_group.get_nth_value(i).unwrap();
                    let filter_block = read_block_data(file, bh)?;
                    let filter = Filter::from_sstable_block(filter_block, v, opt)
                        .ok_or(LError::InvalidFile(format!("unable to decode filter")))?;
                    return Ok(Some(filter));
                }
            }
        }
    }
    Err(LError::InvalidFile(format!("unavailable filter found")))
}

pub(crate) fn read_block_data<S: Storage>(
    file: &mut S,
    handle: BlockHandle,
) -> Result<Bytes, LError> {
    let mut data = Vec::with_capacity(handle.length as usize + BLOCK_TRAILER_LEN);
    data.resize(data.capacity(), 0);
    file.seek(handle.offset)?;
    file.read_exact(&mut data)?;
    Ok(Bytes::from(data))
}

#[derive(Clone, Default)]
pub(crate) struct FilterBlock {
    data: Bytes,
}

impl FilterBlock {
    #[allow(unused)]
    pub fn take_data(self) -> Bytes {
        self.data
    }
}

impl From<Bytes> for FilterBlock {
    fn from(d: Bytes) -> Self {
        Self { data: d }
    }
}

impl Encoding for FilterBlock {
    fn encode<C: Comparator>(&self, dst: &mut BytesMut, _opts: &Opts<C>) -> usize {
        let chechsum = crc32(self.data.as_ref());
        dst.put_slice(self.data.as_ref());
        dst.put_u8(NO_COMPRESSION_BLOCK_TYPE);
        dst.put_u32_le(chechsum);
        self.data.len()
    }

    fn decode<C: Comparator>(src: &mut Bytes, opts: &Opts<C>) -> Result<Self, LError> {
        if src.len() < 5 {
            Err(LError::InvalidFile(format!("invalid filter block")))
        } else {
            Ok(FilterBlock::from(split_block_tracer(src, opts)?))
        }
    }
}

pub(crate) type DataBlockPtr = Arc<DataBlock>;
pub(crate) type DataBlock = DecodedBlock<Bytes>;
pub(crate) type IndexBlock = DecodedBlock<BlockHandle>;

#[derive(Clone, Default)]
pub struct DecodedBlock<T: Clone> {
    pub(crate) entries: Vec<EntryGroup<T>>,
}

impl<T: Clone + Debug> Debug for DecodedBlock<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("DecodedBlock(entries=({:?}))", self.entries))
    }
}

impl<T: Encoding + Clone> Encoding for DecodedBlock<T> {
    fn encode<C: Comparator>(&self, dst: &mut BytesMut, opts: &Opts<C>) -> usize {
        let d = {
            let mut buf = BytesMut::new();
            let mut restarts = vec![0u32];
            let mut s = 0;
            for entry_group in self.entries.iter() {
                s += entry_group.encode(&mut buf, opts);
                restarts.push(s as u32);
            }
            restarts.pop();
            for restart in restarts.iter() {
                buf.put_slice(u32::to_le_bytes(*restart).as_slice());
            }
            buf.put_slice(u32::to_le_bytes(restarts.len() as u32).as_slice());
            buf
        };
        let (data_size, checksum) = if opts.get_compression() {
            let dd = match Encoder::new().compress_vec(d.as_ref()) {
                Ok(c) => c,
                Err(e) => panic!("failed to compress block, err: {:?}", e),
            };
            dst.put_slice(dd.as_slice());
            dst.put_u8(SNAPPY_COMPRESSION_BLOCK_TYPE);
            (dd.len(), crc32(dd.as_ref()))
        } else {
            dst.put_slice(d.as_ref());
            dst.put_u8(NO_COMPRESSION_BLOCK_TYPE);
            (d.len(), crc32(d.as_ref()))
        };
        dst.put_slice(u32::to_le_bytes(checksum).as_slice());
        data_size
    }

    fn decode<C: Comparator>(src: &mut Bytes, opts: &Opts<C>) -> Result<Self, LError> {
        let mut block_data = split_block_tracer(src, opts)?;
        let block_len = block_data.len();
        let num_restarts = u32::from_le_bytes(
            block_data
                .split_off(block_len - 4)
                .as_ref()
                .try_into()
                .unwrap(),
        ) as usize;
        if num_restarts == 0 {
            return Err(LError::InvalidFile(format!(
                "invalid table for no restart points"
            )));
        }
        let mut restarts_data = block_data.split_off(block_data.len() - num_restarts * 4);
        let data_len = block_data.len();
        let (mut entries, mut restarts) = (vec![], vec![]);
        let mut entry_blocks = vec![];
        let mut segments = vec![];
        for i in 0..num_restarts as usize {
            let restart_point =
                u32::from_le_bytes(restarts_data.split_to(4).as_ref().try_into().unwrap());
            restarts.push(restart_point);
            if i > 0 {
                segments.push((restarts[i] - restarts[i - 1]) as usize);
            }
        }
        segments.push(data_len - (restarts[restarts.len() - 1]) as usize);
        for s in segments {
            entry_blocks.push(block_data.split_to(s));
        }
        for mut entry_block in entry_blocks {
            entries.push(EntryGroup::decode(&mut entry_block, opts)?);
        }
        Ok(DecodedBlock { entries: entries })
    }
}

fn split_block_tracer<C: Comparator>(src: &mut Bytes, opts: &Opts<C>) -> Result<Bytes, LError> {
    let len = src.len();
    if len < 5 {
        return Err(LError::InvalidFile(format!(
            "invalid block for too short length"
        )));
    }
    if opts.verify_checksum {
        let expected_checksum = u32::from_le_bytes(src[len - 4..].as_ref().try_into().unwrap());
        let actual_checksum = crc32(src[..len - 5].as_ref());
        if expected_checksum != actual_checksum {
            return Err(LError::InvalidFile(format!(
                "invalid block for checksum mismatch"
            )));
        }
    }
    let compression_type = src[len - 5];
    let _ = src.split_off(len - 5);
    let d = match compression_type {
        NO_COMPRESSION_BLOCK_TYPE => src.split_to(src.len()),
        SNAPPY_COMPRESSION_BLOCK_TYPE => match Decoder::new().decompress_vec(src.as_ref()) {
            Ok(d) => Bytes::from(d),
            Err(_) => {
                return Err(LError::InvalidFile(format!(
                    "invalid block for unable to decompress"
                )))
            }
        },
        o => {
            return Err(LError::InvalidFile(format!(
                "invalid compression type {}",
                o
            )))
        }
    };
    Ok(d)
}

impl<T: Clone> DecodedBlock<T> {
    pub fn len(&self) -> usize {
        self.entries.iter().map(|x| x.len()).sum()
    }

    pub fn search_value<C: Comparator>(&self, key: &[u8], opts: &Opts<C>) -> Result<T, Option<T>> {
        let comparator = opts.get_comparator();
        let uk = InternalKeyRef::from(key).ukey;
        let i = match self.entries.binary_search_by(|x| {
            comparator.compare(InternalKeyRef::from(x.first.key_delta.as_ref()).ukey, uk)
        }) {
            Err(i) => {
                if i == 0 {
                    return Err(None);
                }
                i - 1
            }
            Ok(i) => i,
        };
        self.entries[i].search_value(key, opts)
    }

    pub(crate) fn set<C: Comparator>(&mut self, key: Bytes, value: T, opts: &Opts<C>) {
        if let Some(e) = self.entries.last_mut() {
            if e.len() < opts.get_block_restart_interval() {
                e.push_value(key, value, opts);
                return;
            }
        }
        self.entries.push(EntryGroup {
            first: Entry {
                shared_bytes: 0,
                unshared_bytes: key.len() as u32,
                key_delta: key,
                value: value,
                full_key: OnceCell::new(),
            },
            following: vec![],
        });
    }
}

impl IndexBlock {
    pub(crate) fn get_nth_block_handle(&self, mut i: usize) -> Option<BlockHandle> {
        for entry_group in self.entries.iter() {
            if i < entry_group.len() {
                return entry_group.get_nth_value(i);
            } else {
                i -= entry_group.len();
            }
        }
        None
    }
}

impl DataBlock {
    pub(crate) fn size(&self) -> usize {
        let entry_size =
            |entry: &Entry<Bytes>| -> usize { entry.value.len() + entry.key_delta.len() + 16 };
        self.entries
            .iter()
            .map(|x| entry_size(&x.first) + x.following.iter().map(entry_size).sum::<usize>())
            .sum()
    }

    pub(crate) fn get_nth_entry(&self, mut i: usize) -> Option<(Bytes, Bytes)> {
        for entry in self.entries.iter() {
            if i < entry.len() {
                let v = if i == 0 {
                    (entry.first.key_delta.clone(), entry.first.value.clone())
                } else {
                    let mut key = BytesMut::new();
                    let e = &entry.following[i - 1];
                    key.put_slice(entry.first.key_delta[..e.shared_bytes as usize].as_ref());
                    key.put_slice(e.key_delta.as_ref());
                    (key.freeze(), e.value.clone())
                };
                return Some(v);
            } else {
                i -= entry.len();
            }
        }
        None
    }
}

#[derive(Clone)]
pub(crate) struct Entry<T: Clone> {
    shared_bytes: u32,
    unshared_bytes: u32,
    key_delta: Bytes,
    value: T,
    full_key: OnceCell<Bytes>,
}

impl<T: Clone> Entry<T> {
    pub(crate) fn get_full_key(&self, first_key: &[u8]) -> &Bytes {
        self.full_key.get_or_init(|| {
            let mut d = Vec::with_capacity((self.shared_bytes + self.unshared_bytes) as usize);
            d.extend_from_slice(first_key[..(self.shared_bytes as usize)].as_ref());
            d.extend_from_slice(self.key_delta.as_ref());
            Bytes::from(d)
        })
    }
}

impl<T: Clone + Debug> Debug for Entry<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Entry(shared_bytes={},unshared_bytes={}, key_delta={:?})",
            self.shared_bytes, self.unshared_bytes, self.key_delta
        ))
    }
}

impl<T: Encoding + Clone> Encoding for Entry<T> {
    fn encode<C: Comparator>(&self, dst: &mut BytesMut, opts: &Opts<C>) -> usize {
        let mut value = BytesMut::new();
        let value_len = self.value.encode(&mut value, opts);
        let s = put_uvarint(dst, self.shared_bytes as u64)
            + put_uvarint(dst, self.unshared_bytes as u64)
            + put_uvarint(dst, value_len as u64)
            + self.key_delta.len()
            + value_len;
        dst.put_slice(self.key_delta.as_ref());
        dst.put_slice(value.as_ref());
        s
    }

    fn decode<C: Comparator>(src: &mut Bytes, opts: &Opts<C>) -> Result<Self, LError> {
        let shared_bytes =
            take_uvarint(src).ok_or(LError::InvalidFile(format!("invalid shared_bytes")))? as u32;
        let unshared_bytes =
            take_uvarint(src).ok_or(LError::InvalidFile(format!("invalid unshared_bytes")))? as u32;
        let value_length =
            take_uvarint(src).ok_or(LError::InvalidFile(format!("invalid value_len")))? as u32;
        let key_delta = src.split_to(unshared_bytes as usize);
        let mut v = src.split_to(value_length as usize);
        let value = T::decode(&mut v, opts)?;
        Ok(Self {
            shared_bytes,
            unshared_bytes,
            key_delta,
            value,
            full_key: OnceCell::new(),
        })
    }
}

#[derive(Clone)]
pub(crate) struct EntryGroup<T: Clone> {
    first: Entry<T>,
    following: Vec<Entry<T>>,
}

impl<T: Clone + Debug> Debug for EntryGroup<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "EntryGroup(first={:?}, following={:?})",
            self.first, self.following
        ))
    }
}

impl<T: Clone> EntryGroup<T> {
    #[allow(unused)]
    fn new(first: Entry<T>) -> Self {
        Self {
            first,
            following: vec![],
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.following.len() + 1
    }

    fn get_nth_key(&self, i: usize) -> Option<Bytes> {
        match i {
            0 => Some(self.first.key_delta.clone()),
            o => self.following.get(o - 1).map(|e| {
                let mut key = BytesMut::new();
                key.put_slice(self.first.key_delta[..e.shared_bytes as usize].as_ref());
                key.put_slice(e.key_delta.as_ref());
                key.freeze()
            }),
        }
    }

    fn get_nth_value(&self, i: usize) -> Option<T> {
        match i {
            0 => Some(self.first.value.clone()),
            o => self.following.get(o - 1).map(|x| x.value.clone()),
        }
    }

    pub(crate) fn search_value<C: Comparator>(
        &self,
        key: &[u8],
        opts: &Opts<C>,
    ) -> Result<T, Option<T>> {
        let ikr = InternalKeyRef::from(key);
        let c = opts.get_comparator();
        let first_key = InternalKeyRef::from(self.first.key_delta.as_ref());
        match c.compare(first_key.ukey, ikr.ukey) {
            Ordering::Less => {}
            Ordering::Equal => return Ok(self.first.value.clone()),
            Ordering::Greater => return Err(None),
        }
        let find_result = self.following.binary_search_by(|x| {
            let x_full_key = x.get_full_key(self.first.key_delta.as_ref());
            let ik = match InternalKey::try_from(x_full_key.clone()) {
                Ok(k) => k,
                Err(_e) => return Ordering::Less,
            };
            c.compare(ik.ukey(), ikr.ukey)
        });

        match find_result {
            Err(0) => Err(Some(self.first.value.clone())),
            Err(i) => Err(Some(self.following[i - 1].value.clone())),
            Ok(i) => Ok(self.following[i].value.clone()),
        }
    }

    pub(crate) fn push_value<C: Comparator>(&mut self, mut key: Bytes, value: T, _opts: &Opts<C>) {
        let shared_bytes = prefix_len(&self.first.key_delta, &key);
        let unshared_bytes = if key.len() > shared_bytes {
            key.len() - shared_bytes
        } else {
            0
        };
        if unshared_bytes > 0 {
            let _ = key.split_to(shared_bytes);
        }
        self.following.push(Entry {
            shared_bytes: shared_bytes as u32,
            unshared_bytes: unshared_bytes as u32,
            key_delta: key,
            value: value,
            full_key: OnceCell::new(),
        })
    }
}

fn prefix_len(left: &Bytes, right: &Bytes) -> usize {
    let mut s = 0;
    for i in 0..left.len() {
        if i < right.len() && right[i] == left[i] {
            s += 1;
        } else {
            break;
        }
    }
    s
}

impl<T: Encoding + Clone> Encoding for EntryGroup<T> {
    fn encode<C: Comparator>(&self, dst: &mut BytesMut, opts: &Opts<C>) -> usize {
        self.first.encode(dst, opts)
            + self
                .following
                .iter()
                .map(|x| x.encode(dst, opts))
                .sum::<usize>()
    }

    fn decode<C: Comparator>(src: &mut Bytes, opts: &Opts<C>) -> Result<Self, LError> {
        let first: Entry<T> = Entry::decode(src, opts)?;
        let mut following = vec![];
        while !src.is_empty() {
            following.push(Entry::decode(src, opts)?);
        }
        Ok(Self { first, following })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Filter {
    #[allow(unused)]
    data: Bytes,
    #[allow(unused)]
    offsets: Bytes,
    #[allow(unused)]
    policy: &'static BloomFilter,
    #[allow(unused)]
    shift: u32,
}

impl Filter {
    pub(crate) fn from_sstable_block<C: Comparator>(
        mut data: Bytes,
        policy: &'static BloomFilter,
        opts: &Opts<C>,
    ) -> Option<Self> {
        let mut data = match FilterBlock::decode(&mut data, opts) {
            Err(_) => return None,
            Ok(d) => d.data,
        };
        let data_len = data.len();
        if data_len < 5 {
            return None;
        }
        let last_offset = u32::from_le_bytes(
            data[data_len - 5..data_len - 1]
                .as_ref()
                .try_into()
                .unwrap(),
        );
        if last_offset as u64 > (data_len - 5) as u64 {
            return None;
        }
        let shift = data[data_len - 1] as u32;
        let _ = data.split_off(data_len - 1);
        let offsets = data.split_off(last_offset as usize);
        if offsets.len() & 3 != 0 {
            return None;
        }
        Some(Filter {
            data,
            offsets,
            policy,
            shift,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::compare::BytewiseComparator;
    use crate::key::InternalKeyRef;
    use crate::opts::{Opts, OptsRaw};
    use crate::sstable::format::{
        BlockHandle, DecodedBlock, Encoding, Entry, EntryGroup, FilterBlock, Footer, IndexBlock,
    };
    use bytes::{Bytes, BytesMut};
    use once_cell::sync::OnceCell;

    #[test]
    fn test_footer() {
        let opts = Opts::<BytewiseComparator>::default();
        let mut buf = BytesMut::new();
        let footer = Footer::new(BlockHandle::new(1, 2), BlockHandle::new(3, 4));
        footer.encode(&mut buf, &opts);
        assert_eq!(buf.len(), 48);
        let mut buf = buf.freeze();
        let f = Footer::decode(&mut buf, &opts);
        assert!(f.is_ok());
        let f = f.unwrap();
        assert_eq!(f.magic, footer.magic);
        assert_eq!(f.index_handle, footer.index_handle);
        assert_eq!(f.metaindex_handle, footer.metaindex_handle);
    }

    #[test]
    fn test_block_handle() {
        let opts = Opts::<BytewiseComparator>::default();
        let mut buf = BytesMut::new();
        let bhs = vec![
            BlockHandle::new(1, 2),
            BlockHandle::new(1 << 18, 2 << 18),
            BlockHandle::new(1 << 34, 2 << 34),
            BlockHandle::new(1 << 62, 2 << 62),
        ];
        for bh in bhs.iter() {
            assert_eq!(20, bh.encode(&mut buf, &opts));
        }
        let mut dd = buf.freeze();
        for bh in bhs.iter() {
            let bb = BlockHandle::decode(&mut dd, &opts).unwrap();
            assert_eq!(bh.length, bb.length);
            assert_eq!(bh.offset, bb.offset);
        }
    }

    #[test]
    fn test_entry_group() {
        let opts = Opts::<BytewiseComparator>::default();
        let kvs: Vec<(Bytes, Bytes)> = (11..99)
            .map(|x| {
                (
                    InternalKeyRef::from((format!("key:{}", x).as_bytes(), x as u64))
                        .to_owned()
                        .borrow_inner()
                        .clone(),
                    format!("value:{}", x),
                )
            })
            .map(|(k, v)| (Bytes::from(k), Bytes::from(v)))
            .collect();
        let mut eg = EntryGroup::new(Entry {
            shared_bytes: 0,
            unshared_bytes: 14,
            key_delta: InternalKeyRef::from((format!("key:{}", 10).as_bytes(), 10 as u64))
                .to_owned()
                .borrow_inner()
                .clone(),
            value: Bytes::from("value:10".to_string()),
            full_key: OnceCell::new(),
        });
        for (k, v) in kvs.iter() {
            eg.push_value(k.clone(), v.clone(), &opts);
        }
        assert_eq!(eg.len(), 89);
        let key = InternalKeyRef::from(("key:10".as_bytes(), 10)).to_owned();
        assert_eq!(
            eg.search_value(key.as_ref(), &opts),
            Ok(Bytes::from("value:10".to_string()))
        );
        for i in 1..89 {
            assert_eq!(
                eg.search_value(&kvs[i - 1].0.as_ref(), &opts),
                Ok(kvs[i - 1].1.clone())
            )
        }
        let mut buf = BytesMut::new();
        eg.encode(&mut buf, &opts);
        let mut buf = buf.freeze();
        let eg1: EntryGroup<Bytes> = EntryGroup::decode(&mut buf, &opts).unwrap();
        assert_eq!(eg1.len(), eg.len());
        for i in 0..eg.len() {
            assert_eq!(eg1.get_nth_key(i), eg.get_nth_key(i));
        }
    }

    #[test]
    fn test_decoded_block() {
        let opts = new_opt_with_checksum();
        let mut db: DecodedBlock<Bytes> = DecodedBlock::default();
        let kvs: Vec<(Bytes, Bytes)> = (11..99)
            .map(|x| {
                (
                    InternalKeyRef::from((format!("key:{}", x).as_bytes(), x as u64))
                        .to_owned()
                        .borrow_inner()
                        .clone(),
                    format!("value:{}", x),
                )
            })
            .map(|(k, v)| (k, Bytes::from(v)))
            .collect();
        for (k, v) in kvs.iter() {
            db.set(k.clone(), v.clone(), &opts);
        }
        assert_eq!(db.len(), 88);
        for i in 0..88 {
            assert_eq!(db.get_nth_entry(i), Some(kvs[i].clone()));
            assert_eq!(db.search_value(&kvs[i].0, &opts), Ok(kvs[i].1.clone()));
        }
        let mut buf = BytesMut::new();
        db.encode(&mut buf, &opts);

        let mut buf = buf.freeze();
        let db2: DecodedBlock<Bytes> = DecodedBlock::decode(&mut buf, &opts).unwrap();
        assert_eq!(db.len(), db2.len());
        for i in 0..db.len() {
            assert_eq!(db.get_nth_entry(i), db2.get_nth_entry(i));
        }
    }

    #[test]
    fn test_index_block() {
        let opts = new_opt_with_checksum();
        let mut ib: IndexBlock = DecodedBlock::default();
        let bhs: Vec<(Bytes, BlockHandle)> = (100..199)
            .map(|x| {
                (
                    InternalKeyRef::from((format!("key:{}", x).as_bytes(), x as u64))
                        .to_owned()
                        .borrow_inner()
                        .clone(),
                    BlockHandle::new(x as u64, x as u64),
                )
            })
            .collect();
        for (key, bh) in bhs.iter() {
            ib.set(key.clone(), bh.clone(), &opts);
        }
        for i in 0..99 {
            assert_eq!(ib.get_nth_block_handle(i), Some(bhs[i].1));
            assert_eq!(ib.search_value(&bhs[i].0, &opts), Ok(bhs[i].1));
        }
        let mut buf = BytesMut::new();
        ib.encode(&mut buf, &opts);

        let mut buf = buf.freeze();
        let ib2: IndexBlock = DecodedBlock::decode(&mut buf, &opts).unwrap();
        assert_eq!(ib.len(), ib2.len());
        for i in 0..ib.len() {
            assert_eq!(ib.get_nth_block_handle(i), ib2.get_nth_block_handle(i));
        }
    }

    #[test]
    fn test_filter_block() {
        let opts = new_opt_with_checksum();
        let fb = FilterBlock::from(Bytes::from("test_filter_block".to_string()));
        let mut buf = BytesMut::new();
        fb.encode(&mut buf, &opts);

        let mut buf = buf.freeze();
        let fb2 = FilterBlock::decode(&mut buf, &opts).unwrap();
        assert_eq!(fb2.data, fb.data);
    }

    fn new_opt_with_checksum() -> Opts<BytewiseComparator> {
        let mut optsr = OptsRaw::<BytewiseComparator>::default();
        optsr.verify_checksum = true;
        Opts::new(optsr)
    }
}
