use crate::compare::{BytewiseComparator, Comparator};
use crate::utils::any::Any;
use bytes::Bytes;
use std::sync::Arc;

pub type Opts<C> = Arc<OptsRaw<C>>;

pub struct OptsRaw<T> {
    pub filter_name: Option<String>,
    pub verify_checksum: bool,
    pub block_cache_limit: usize,
    pub block_restart_interval: usize,
    pub block_size: usize,
    pub compression: bool,
    pub comparer: T,
    pub error_if_db_exists: bool,
    pub write_buffer_size: u64,
    pub max_file_size: u64,
    pub compact_hook: (Any, CompactHook),
}

pub fn default_opts() -> Opts<BytewiseComparator> {
    Arc::new(OptsRaw::default())
}

impl<C: Comparator + Default> Default for OptsRaw<C> {
    fn default() -> Self {
        Self {
            filter_name: None,
            verify_checksum: true,
            block_cache_limit: 67108864, // 64mb
            block_restart_interval: 16,
            block_size: 4096,
            compression: true,
            comparer: C::default(),
            error_if_db_exists: false,
            write_buffer_size: 4 * 1024 * 1024,
            max_file_size: 4 * 1024 * 1024,
            compact_hook: (Any::new(()), empty_compact_hook),
        }
    }
}

impl<T: Comparator> OptsRaw<T> {
    pub fn get_comparator(&self) -> T {
        self.comparer.clone()
    }

    pub fn get_block_restart_interval(&self) -> usize {
        if self.block_restart_interval == 0 {
            16
        } else {
            self.block_restart_interval
        }
    }

    pub fn get_block_size(&self) -> usize {
        if self.block_size <= 0 {
            4096
        } else {
            self.block_size
        }
    }

    pub fn get_block_cache_limit(&self) -> usize {
        self.block_cache_limit
    }

    pub fn get_compression(&self) -> bool {
        self.compression
    }

    pub fn get_write_buffer_size(&self) -> u64 {
        self.write_buffer_size
    }

    pub fn get_error_if_db_exists(&self) -> bool {
        self.error_if_db_exists
    }

    pub fn get_max_file_size(&self) -> u64 {
        self.max_file_size
    }

    pub fn get_compact_hook(&self) -> CompactHook {
        self.compact_hook.1
    }
}

type CompactHook = fn(&Any, &[u8], u64, Option<Bytes>) -> bool;
pub fn empty_compact_hook(_a: &Any, _uk: &[u8], _seq_num: u64, _value: Option<Bytes>) -> bool {
    true
}
