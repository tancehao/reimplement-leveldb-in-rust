use crate::LError::{Internal, LockError};
use std::io::Error;
use std::sync::PoisonError;
pub mod batch;
pub mod compaction;
pub mod compare;
pub mod db;
pub mod filename;
pub mod filter;
pub mod io;
pub mod key;
pub mod memtable;
pub mod opts;
pub mod sstable;
pub mod utils;
pub mod version;
pub mod wal;

const L0_COMPACTION_TRIGGER: usize = 4;
const L0_SLOWDOWN_WRITES_TRIGGER: usize = 8;
const L0_STOP_WRITES_TRIGGER: usize = 12;

#[derive(Debug)]
pub enum LError {
    IO(Error),
    InvalidFile(String),
    Compaction(String),
    UnsupportedFilter(String),
    Filter(String),
    Internal(String),
    LockError(String),
    UnsupportedSystem(String),
    InvalidInternalKey(Vec<u8>),
}

impl Clone for LError {
    fn clone(&self) -> Self {
        match self {
            LError::IO(_) => unimplemented!(),
            others => others.clone(),
        }
    }
}

impl From<Error> for LError {
    fn from(e: Error) -> Self {
        Self::IO(e)
    }
}

impl<T> From<PoisonError<T>> for LError {
    fn from(value: PoisonError<T>) -> Self {
        LockError(value.to_string())
    }
}

impl From<std::fmt::Error> for LError {
    fn from(value: std::fmt::Error) -> Self {
        Internal(format!("{}", value))
    }
}
