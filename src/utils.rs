use std::ops::{Deref, DerefMut};
use std::sync::PoisonError;

pub mod any;
pub mod call_on_drop;
pub mod crc;
pub mod varint;
pub mod lru;

#[allow(unused)]
pub(crate) struct DebugMutex<T> {
    l: std::sync::Mutex<T>,
}

impl<T> DebugMutex<T> {
    #[allow(unused)]
    pub fn new(l: T) -> Self {
        Self {
            l: std::sync::Mutex::new(l),
        }
    }

    #[allow(unused)]
    pub fn lock(&self) -> std::sync::LockResult<DebugMutexGuard<T>> {
        match self.l.lock() {
            Err(e) => Err(PoisonError::new(DebugMutexGuard { g: e.into_inner() })),
            Ok(g) => {
                println!(
                    "[DebugMutex] acquired mutex, thread: {:?}",
                    std::thread::current().id()
                );
                Ok(DebugMutexGuard { g })
            }
        }
    }
}

#[allow(unused)]
pub(crate) struct DebugMutexGuard<'a, T: ?Sized> {
    g: std::sync::MutexGuard<'a, T>,
}

impl<'a, T: ?Sized> Drop for DebugMutexGuard<'a, T> {
    fn drop(&mut self) {
        println!(
            "[DebugMutexGuard] releasing mutex, thread: {:?}",
            std::thread::current().id()
        );
    }
}

impl<'a, T: ?Sized> Deref for DebugMutexGuard<'a, T> {
    type Target = std::sync::MutexGuard<'a, T>;

    fn deref(&self) -> &Self::Target {
        &self.g
    }
}

impl<'a, T: ?Sized> DerefMut for DebugMutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.g
    }
}

#[cfg(debug_assertions)]
#[allow(unused)]
pub(crate) type Mutex<T> = DebugMutex<T>;
#[cfg(debug_assertions)]
#[allow(unused)]
pub(crate) type MutexGuard<'a, T> = DebugMutexGuard<'a, T>;

#[cfg(not(debug_assertions))]
#[allow(unused)]
pub(crate) type Mutex<T> = std::sync::Mutex<T>;
#[cfg(not(debug_assertions))]
#[allow(unused)]
pub(crate) type MutexGuard<'a, T> = std::sync::MutexGuard<'a, T>;
