use std::cmp::Ordering;
use std::fmt::Debug;

pub trait Comparator: Send + Sync + Clone + Debug + Copy + 'static {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

    fn name(&self) -> &'static str;
}

#[derive(Default, Clone, Copy, Debug)]
pub struct BytewiseComparator {}

impl Comparator for BytewiseComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn name(&self) -> &'static str {
        "BytewiseComparator"
    }
}
