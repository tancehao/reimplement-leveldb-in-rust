use std::cmp::Ordering;
use std::fmt::Debug;

pub static BYTEWISE_COMPARATOR: &'static dyn Comparator =
    &BytewiseComparator {} as &'static dyn Comparator;

pub trait Comparator: Send + Sync + Debug + 'static {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

    fn name(&self) -> &'static str;
}

pub type ComparatorImpl = &'static dyn Comparator;

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
