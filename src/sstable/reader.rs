use crate::compare::Comparator;
use crate::db::DBScanner;
use crate::io::{Encoding, Storage};
use crate::key::InternalKey;
use crate::opts::Opts;
use crate::sstable::format::FOOTER_SIZE;
use crate::sstable::format::{
    read_block_data, read_filter_data, BlockHandle, DataBlock, DataBlockPtr, Filter, Footer,
    IndexBlock,
};
use crate::LError;
use bytes::Bytes;
use linked_hash_map_rs::LinkedHashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::Mutex;

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
    pub fn open<C: Comparator>(mut file: S, num: u64, opts: &Opts<C>) -> Result<Self, LError> {
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

    pub(crate) fn load_block<C: Comparator>(
        &self,
        handle: BlockHandle,
        opt: &Opts<C>,
    ) -> Result<DataBlockPtr, LError> {
        if let Some(v) = self.shared_cache.lock()?.get(self.num, handle) {
            return Ok(v);
        }
        let mut file = self.f.lock()?;
        // we should query the cache again, because this method may be concurrently called
        if let Some(v) = self.shared_cache.lock()?.get(self.num, handle) {
            return Ok(v);
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
        self.shared_cache.lock()?.set(self.num, handle, db.clone());
        Ok(db)
    }

    pub(crate) fn get<C: Comparator>(
        &self,
        key: &[u8],
        opts: &Opts<C>,
    ) -> Result<Option<Bytes>, LError> {
        if let Some(_f) = self.filter.as_ref() {
            // TODO need to use bloom filter
        }
        let block_handle = match self.index.search_value(key, opts) {
            Ok(bh) => bh,
            Err(Some(bh)) => bh,
            Err(None) => return Ok(None),
        };
        match self.load_block(block_handle, opts)?.search_value(key, opts) {
            Ok(val) => Ok(Some(val)),
            _ => Ok(None),
        }
    }
}

pub struct SSTableScanner<C: Comparator, S: Storage> {
    reader: SSTableReader<S>,
    opts: Opts<C>,
    current_index: usize,
    current_entry: usize,
}
impl<C: Comparator, S: Storage> SSTableScanner<C, S> {
    pub fn new(reader: SSTableReader<S>, opts: Opts<C>) -> Self {
        Self {
            reader,
            opts,
            current_entry: 0,
            current_index: 0,
        }
    }
}

impl<C: Comparator, S: Storage> DBScanner for SSTableScanner<C, S> {
    fn next(&mut self) -> Result<Option<(InternalKey, Bytes)>, LError> {
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

pub(crate) type BlockCache = Arc<Mutex<LRUCache>>;

// TODO adapt for generic types
#[derive(Default)]
pub(crate) struct LRUCache {
    cache: LinkedHashMap<(u64, BlockHandle), Arc<DataBlock>>,
    size: usize,
    size_limit: usize,
}

impl LRUCache {
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

    pub fn get(&mut self, file_number: u64, bh: BlockHandle) -> Option<Arc<DataBlock>> {
        self.cache
            .move_to_back(&(file_number, bh))
            .map(|(_, db)| db.clone())
    }

    pub fn set(&mut self, file_number: u64, bh: BlockHandle, db: Arc<DataBlock>) {
        let key = (file_number, bh);
        let size = match self.cache.get(&key) {
            None => 0,
            Some(v) => {
                let s = v.size();
                self.cache.remove(&key);
                s
            }
        };
        self.size = self.size - size + db.size();
        self.cache.push_back(key, db);
        while self.size > self.size_limit {
            match self.cache.pop_front() {
                None => break,
                Some((_, db)) => {
                    self.size -= db.size();
                }
            }
        }
    }
}
