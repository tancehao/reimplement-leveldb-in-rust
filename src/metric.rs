use lazy_static::lazy_static;
use prometheus::{register_histogram, Histogram};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

lazy_static! {
    pub static ref COSTS: HashMap<&'static str, Histogram> = {
        let metrics = vec![
            "make_room",
            "lock_wal",
            "append_wal",
            "compact_mem_table",
            "compact_disks",
        ];
        let mut costs = HashMap::new();
        for name in metrics {
            costs.insert(
                name,
                register_histogram!(
                    name,
                    name,
                    vec![
                        500.0, 1000.0, 3000.0, 10000.0, 20000.0, 30000.0, 40000.0, 50000.0,
                        80000.0, 100000.0
                    ]
                )
                .unwrap(),
            );
        }
        costs
    };
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Metric {
    pub(crate) threads: Arc<AtomicUsize>,
    pub(crate) opened_files: Arc<AtomicUsize>,
    pub(crate) sst_files: Arc<AtomicUsize>,
    #[allow(unused)]
    pub(crate) sst_size: Arc<AtomicU64>,
    pub(crate) wal_files: Arc<AtomicUsize>,
}

impl Metric {
    pub(crate) fn incr_threads_cnt(&self) {
        self.threads.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn decr_threads_cnt(&self) {
        self.threads.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn incr_opened_files(&self) {
        self.opened_files.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(unused)]
    pub(crate) fn decr_opened_files(&self) {
        self.opened_files.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn incr_wal_files(&self) {
        self.wal_files.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn decr_wal_files(&self) {
        self.wal_files.fetch_sub(1, Ordering::Relaxed);
    }

    #[allow(unused)]
    pub(crate) fn incr_sst_files(&self) {
        self.sst_files.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn decr_sst_files(&self) {
        self.sst_files.fetch_sub(1, Ordering::Relaxed);
    }
}
