use crate::db::DBScanner;
use crate::io::{Encoding, Storage};
use crate::key::InternalKey;
use crate::opts::Opts;
use crate::sstable::format::FOOTER_SIZE;
use crate::sstable::format::{
    read_block_data, read_filter_data, BlockHandle, DataBlock, DataBlockPtr, Filter, Footer,
    IndexBlock,
};
use crate::utils::lru::{CacheSize, LRUCache};
use crate::LError;
use bytes::Bytes;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::Mutex;

pub(crate) type SSTEntry = (InternalKey, Bytes);

// TODO: make it private
pub struct SSTableReader<S: Storage> {
    pub num: u64,
    pub(crate) f: Arc<Mutex<S>>,
    pub(crate) shared_cache: BlockCache,
    pub(crate) index: IndexBlock,
    pub(crate) filter: Option<Filter>,
}

impl<S: Storage> Clone for SSTableReader<S> {
    fn clone(&self) -> Self {
        Self {
            num: self.num,
            f: self.f.clone(),
            shared_cache: self.shared_cache.clone(),
            index: self.index.clone(),
            filter: self.filter.clone(),
        }
    }
}

impl<S: Storage> Debug for SSTableReader<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "SSTable(num={}, blocks={})",
            self.num,
            self.index.entries.len()
        ))
    }
}

impl<S: Storage> SSTableReader<S> {
    pub fn open(mut file: S, num: u64, opts: &Opts) -> Result<Self, LError> {
        let size = file.size()? as usize;
        let mut footer_raw = [0u8; FOOTER_SIZE];
        if size < footer_raw.len() {
            return Err(LError::InvalidFile(format!("file size is only {}", size)));
        }
        let _ = file.seek((size - FOOTER_SIZE) as u64)?;
        file.read(&mut footer_raw)?;
        let footer = Footer::decode(&mut Bytes::from(footer_raw[..].to_vec()), opts)?;

        let filter_meta_index_block = read_block_data(&mut file, footer.metaindex_handle)?;
        let filter = read_filter_data(&mut file, filter_meta_index_block, opts)?;

        let mut index_block = read_block_data(&mut file, footer.index_handle)?;
        let index = IndexBlock::decode(&mut index_block, opts)?;
        Ok(SSTableReader {
            f: Arc::new(Mutex::new(file)),
            shared_cache: LRUCache::new(0),
            num,
            index,
            filter,
        })
    }

    pub(crate) fn set_cache_handle(&mut self, cache: BlockCache) {
        self.shared_cache = cache;
    }

    pub(crate) fn load_block(
        &self,
        handle: BlockHandle,
        opt: &Opts,
    ) -> Result<DataBlockPtr, LError> {
        if let Some(v) = self.shared_cache.lock()?.get(&(self.num, handle)) {
            return Ok(v.clone());
        }
        let mut file = self.f.lock()?;
        // we should query the cache again, because this method may be concurrently called
        if let Some(v) = self.shared_cache.lock()?.get(&(self.num, handle)) {
            return Ok(v.clone());
        }

        // load from the file actually
        let mut d = match read_block_data(&mut *file, handle) {
            Err(e) => {
                println!("[load_block] failed to read_block_data: {:?}", e);
                return Err(e);
            }
            Ok(d) => d,
        };
        let db = Arc::new(DataBlock::decode(&mut d, opt)?);
        self.shared_cache
            .lock()?
            .set((self.num, handle), db.clone());
        Ok(db)
    }

    pub(crate) fn get(&self, ukey: &[u8], opts: &Opts) -> Result<Option<SSTEntry>, LError> {
        let block_handle = match self.index.search_value(ukey, opts) {
            Ok((_, bh)) => bh,
            Err(Some(bh)) => bh,
            Err(None) => return Ok(None),
        };
        if let Some(filter) = self.filter.as_ref() {
            if !filter.may_contain(block_handle.offset(), ukey) {
                return Ok(None);
            }
        }
        let r = self
            .load_block(block_handle, opts)?
            .search_value(ukey, opts);
        match r {
            Ok((key, val)) => {
                let ik = InternalKey::try_from(key)?;
                Ok(Some((ik, val)))
            }
            _ => Ok(None),
        }
    }
}

pub struct SSTableScanner<S: Storage> {
    reader: SSTableReader<S>,
    opts: Opts,
    current_index: usize,
    current_entry: usize,
}
impl<S: Storage> SSTableScanner<S> {
    pub fn new(reader: SSTableReader<S>, opts: Opts) -> Self {
        Self {
            reader,
            opts,
            current_entry: 0,
            current_index: 0,
        }
    }
}

impl<S: Storage> DBScanner for SSTableScanner<S> {
    fn next(&mut self) -> Result<Option<SSTEntry>, LError> {
        loop {
            match self.reader.index.get_nth_block_handle(self.current_index) {
                None => return Ok(None),
                Some(bh) => {
                    let db = match self.reader.load_block(bh, &self.opts) {
                        Err(e) => {
                            println!("[SSTableIter.next] failed to load block: {:?}", e);
                            return Err(e);
                        }
                        Ok(d) => d,
                    };
                    match db.get_nth_entry(self.current_entry) {
                        Some((k, v)) => {
                            self.current_entry += 1;
                            return Ok(Some((k.try_into()?, v)));
                        }
                        None => {
                            self.current_entry = 0;
                            self.current_index += 1;
                        }
                    }
                }
            }
        }
    }
}

impl CacheSize for (u64, BlockHandle) {
    fn appropriate_size(&self) -> usize {
        24
    }
}

impl CacheSize for DataBlockPtr {
    fn appropriate_size(&self) -> usize {
        self.size()
    }
}

pub(crate) type BlockCache = Arc<Mutex<LRUCache<(u64, BlockHandle), DataBlockPtr>>>;
