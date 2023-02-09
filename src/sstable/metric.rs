use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

#[derive(Clone, Debug, Default)]
pub(crate) struct Metric {
    pub(crate) threads: Arc<AtomicUsize>,
}