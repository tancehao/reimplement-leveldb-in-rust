use crate::compare::{ComparatorImpl, BYTEWISE_COMPARATOR};
use crate::db::Snapshot;
use crate::key::InternalKeyComparator;
use std::sync::Arc;

pub type Opts = Arc<OptsRaw>;

pub struct OptsRaw {
    pub filter_name: Option<String>,
    pub verify_checksum: bool,
    pub block_cache_limit: usize,
    pub block_restart_interval: usize,
    pub block_size: usize,
    pub compression: bool,
    pub comparer: ComparatorImpl,
    pub error_if_db_exists: bool,
    pub write_buffer_size: u64,
    pub max_file_size: u64,
    pub flush_wal: bool,
    pub tiered_parallel: bool,
    pub enable_metrics_server: bool,
}

pub fn default_opts() -> Opts {
    Arc::new(OptsRaw::default())
}

impl Default for OptsRaw {
    fn default() -> Self {
        Self {
            filter_name: None,
            verify_checksum: true,
            block_cache_limit: 67108864, // 64mb
            block_restart_interval: 16,
            block_size: 4096,
            compression: true,
            comparer: BYTEWISE_COMPARATOR,
            error_if_db_exists: false,
            write_buffer_size: 4 * 1024 * 1024,
            max_file_size: 4 * 1024 * 1024,
            flush_wal: true,
            tiered_parallel: true,
            enable_metrics_server: true,
        }
    }
}

impl OptsRaw {
    pub fn get_ucmp(&self) -> ComparatorImpl {
        self.comparer
    }

    pub fn get_icmp(&self) -> InternalKeyComparator {
        InternalKeyComparator { u: self.comparer }
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

    pub fn get_flush_wal(&self) -> bool {
        self.flush_wal
    }

    pub fn get_tiered_parallel(&self) -> bool {
        self.tiered_parallel
    }

    pub fn get_enable_metrics_server(&self) -> bool {
        self.enable_metrics_server
    }
}

#[derive(Clone, Debug)]
pub struct ReadOptions {
    snapshot: Option<Snapshot>,
    verify_checksum: bool,
    fill_cache: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        ReadOptions {
            snapshot: None,
            verify_checksum: true,
            fill_cache: true,
        }
    }
}

impl ReadOptions {
    pub fn set_verify_checksum(&mut self, v: bool) {
        self.verify_checksum = v;
    }

    pub fn set_fill_cache(&mut self, v: bool) {
        self.fill_cache = v;
    }

    pub fn set_snapshot(&mut self, snapshot: Snapshot) {
        self.snapshot = Some(snapshot);
    }

    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    pub fn verify_checksum(&self) -> bool {
        self.verify_checksum
    }

    pub fn fill_cache(&self) -> bool {
        self.fill_cache
    }
}

#[derive(Default, Clone, Debug)]
pub struct WriteOptions {
    sync: Option<bool>,
}

impl WriteOptions {
    pub fn sync(&self) -> Option<bool> {
        self.sync
    }

    pub fn set_sync(&mut self, s: bool) {
        self.sync = Some(s);
    }
}
