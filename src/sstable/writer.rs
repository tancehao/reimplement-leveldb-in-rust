use crate::compare::Comparator;
use crate::filter::{BloomFilter, FilterPolicy, BLOOM_FILTER};
use crate::io::{Encoding, Storage};
use crate::key::InternalKey;
use crate::opts::Opts;
use crate::sstable::format::{BlockHandle, DataBlock, Filter, FilterBlock, Footer, IndexBlock};
use crate::sstable::reader::SSTableReader;
use crate::utils::lru::LRUCache;
use crate::LError;
use bytes::{BufMut, Bytes, BytesMut};
use std::cmp::Ordering;
use std::sync::{Arc, Mutex};

pub struct SSTableWriter<S: Storage> {
    num: u64,
    file: S,
    opts: Opts,
    wrote_size: u64,
    last_key: Option<InternalKey>,
    data_block: DataBlock,
    offset: u64,
    index_block: IndexBlock,
    filter: FilterWriter,
    filter_block: Option<FilterBlock>,
    finish: bool,
}

impl<S: Storage> SSTableWriter<S> {
    pub(crate) fn new(num: u64, file: S, opts: Opts) -> Self {
        Self {
            num,
            file,
            opts,
            wrote_size: 0,
            last_key: None,
            data_block: DataBlock::default(),
            offset: 0,
            index_block: IndexBlock::default(),
            filter: FilterWriter::default(),
            filter_block: None,
            finish: false,
        }
    }

    pub(crate) fn set(&mut self, key: &InternalKey, value: Bytes) -> Result<usize, LError> {
        match self.last_key.as_ref() {
            None => {}
            Some(k) => {
                if let Ordering::Greater = self.opts.get_icmp().compare(k.as_ref(), key.as_ref()) {
                    return Err(LError::Internal(
                        "keys are set in non-increasing order".into(),
                    ));
                }
            }
        }
        self.filter.append_key(key.clone());
        self.last_key = Some(key.clone());
        self.data_block.set(key.clone().into(), value, &self.opts);
        if self.data_block.size() >= self.opts.get_block_size() {
            self.write_block()?;
        }
        Ok(self.offset as usize)
    }

    // TODO: finish filter block
    fn write_block(&mut self) -> Result<(), LError> {
        let mut tmp = BytesMut::new();
        let (key, _) = self.data_block.get_nth_entry(0).unwrap();
        let length = self.data_block.encode(&mut tmp, &self.opts) as u64;
        self.write(tmp.as_ref())?;
        self.index_block
            .set(key, BlockHandle::new(self.offset, length), &self.opts);
        self.offset += tmp.len() as u64;
        self.data_block = DataBlock::default();
        self.filter.finish_block(self.offset)?;
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> Result<(), LError> {
        if self.finish {
            return Ok(());
        }
        if self.data_block.get_nth_entry(0).is_some() {
            self.write_block()?;
        }
        // meta blocks
        let b = self.filter.finish()?;
        let fb = FilterBlock::from(b);
        let mut tmp = BytesMut::new();
        let filter_len = fb.encode(&mut tmp, &self.opts);
        self.filter_block = Some(fb);
        self.write(tmp.as_ref())?;
        let filter_block_handle = BlockHandle::new(self.offset, filter_len as u64);
        self.offset += tmp.len() as u64;
        tmp.clear();

        // meta index block
        let mut metaindex = IndexBlock::default();
        metaindex.set(
            Bytes::from(format!("filter.{}", BloomFilter::name())),
            filter_block_handle,
            &self.opts,
        );
        let meta_index_size = metaindex.encode(&mut tmp, &self.opts);
        self.write(tmp.as_ref())?;
        let metaindexhandle = BlockHandle::new(self.offset, meta_index_size as u64);
        self.offset += tmp.len() as u64;
        tmp.clear();

        // index block
        let index_size = self.index_block.encode(&mut tmp, &self.opts);
        self.write(tmp.as_ref())?;
        let indexhandle = BlockHandle::new(self.offset, index_size as u64);
        self.offset += tmp.len() as u64;
        tmp.clear();

        // footer
        let f = Footer::new(indexhandle, metaindexhandle);
        f.encode(&mut tmp, &self.opts);
        self.write(tmp.as_ref())?;

        self.finish = true;
        Ok(())
    }

    fn write(&mut self, data: &[u8]) -> Result<(), LError> {
        self.file.write_all(data)?;
        self.wrote_size += data.len() as u64;
        Ok(())
    }

    pub(crate) fn freeze(mut self) -> Result<SSTableReader<S>, LError> {
        if !self.finish {
            self.finish()?;
        }
        let filter = match self.filter_block {
            None => None,
            Some(b) => {
                let mut dst = BytesMut::new();
                b.encode(&mut dst, &self.opts);
                Filter::from_sstable_block(dst.freeze(), &BLOOM_FILTER, &self.opts)
            }
        };
        // TODO: the underlying file should be set non-writable
        Ok(SSTableReader {
            num: self.num,
            f: Arc::new(Mutex::new(self.file)),
            shared_cache: LRUCache::new(0),
            index: Arc::new(self.index_block),
            filter: filter,
        })
    }

    pub(crate) fn wrote_size(&self) -> u64 {
        self.wrote_size
    }
}

const FILTER_BASE_LOG: u8 = 11;

#[derive(Default, Debug)]
pub(crate) struct FilterWriter {
    block: Vec<InternalKey>,
    data: BytesMut,
    offsets: Vec<u32>,
}

impl FilterWriter {
    fn append_key(&mut self, key: InternalKey) {
        self.block.push(key);
    }

    fn append_offset(&mut self) -> Result<(), LError> {
        let o = self.data.len() as u64;
        if o > (1u64 << 32 - 1) {
            return Err(LError::Filter("filter data is too long".into()));
        }
        self.offsets.push(o as u32);
        Ok(())
    }

    fn emit(&mut self) -> Result<(), LError> {
        self.append_offset()?;
        if self.block.is_empty() {
            return Ok(());
        }
        let ukeys: Vec<&[u8]> = self.block.iter().map(|x| x.ukey()).collect();
        self.data = BLOOM_FILTER.append_filter(&mut self.data, ukeys.as_ref());
        self.block.clear();
        Ok(())
    }

    fn finish_block(&mut self, block_offset: u64) -> Result<(), LError> {
        let i = block_offset >> FILTER_BASE_LOG;
        while i > self.offsets.len() as u64 {
            self.emit()?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<Bytes, LError> {
        if !self.block.is_empty() {
            self.emit()?;
        }
        self.append_offset()?;
        for x in self.offsets.iter() {
            let x = u32::to_le_bytes(*x);
            self.data.put_slice(x.as_slice());
        }
        self.data.put_u8(FILTER_BASE_LOG);
        Ok(self.data.clone().freeze())
    }
}

#[cfg(test)]
mod test {
    use crate::filter::BLOOM_FILTER;
    use crate::io::Encoding;
    use crate::key::InternalKeyRef;
    use crate::opts::{Opts, OptsRaw};
    use crate::sstable::format::{Filter, FilterBlock};
    use crate::sstable::writer::FilterWriter;
    use bytes::BytesMut;

    #[test]
    fn test_filter() {
        let opts = Opts::new(OptsRaw::default());
        let mut fw = FilterWriter::default();
        let mut ikeys = vec![];
        for i in 0..10 {
            ikeys.push(InternalKeyRef::from((format!("key{}", i).as_bytes(), i)).to_owned());
        }
        for ik in ikeys.iter() {
            fw.append_key(ik.clone());
        }
        let f = fw.finish();
        assert!(f.is_ok());
        let fb = FilterBlock::from(f.unwrap());
        let mut dd = BytesMut::new();
        fb.encode(&mut dd, &opts);
        let fb = Filter::from_sstable_block(dd.freeze(), &BLOOM_FILTER, &opts);
        assert!(fb.is_some());
        let fb = fb.unwrap();
        for ik in ikeys.iter() {
            assert!(fb.may_contain(0, ik.ukey()));
        }
    }
}
