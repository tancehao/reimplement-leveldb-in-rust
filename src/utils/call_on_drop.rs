// Imitate golang's defer mechanism

#[macro_export]
macro_rules! call_on_drop {
    ($f:expr) => {
        CallOnDrop {
            f: Some(Box::new(move || -> () { $f }) as Box<dyn FnOnce() -> () + Send + Sync>),
        }
    };
}

#[macro_export]
macro_rules! unregister {
    ($f:expr) => {
        $f.cancel()
    };
}

pub struct CallOnDrop {
    pub(crate) f: Option<Box<dyn FnOnce() -> () + Send + Sync>>,
}

impl Drop for CallOnDrop {
    fn drop(&mut self) {
        if let Some(f) = self.f.take() {
            f();
        }
    }
}

impl CallOnDrop {
    #[allow(unused)]
    pub(crate) fn cancel(&mut self) {
        self.f = None;
    }
}

#[cfg(test)]
mod test {
    use super::CallOnDrop;
    use std::sync::atomic::{AtomicI8, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_call_on_drop() {
        let x = Arc::new(AtomicI8::new(0));
        let xc = x.clone();
        {
            let _f = call_on_drop!({
                xc.fetch_add(1, Ordering::Relaxed);
            });
            assert_eq!(x.load(Ordering::Relaxed), 0);
        }
        assert_eq!(x.load(Ordering::Relaxed), 1);
        let xcc = x.clone();
        {
            let mut f = call_on_drop!({
                xcc.fetch_add(1, Ordering::Relaxed);
            });
            assert_eq!(x.load(Ordering::Relaxed), 1);
            unregister!(f);
            assert_eq!(x.load(Ordering::Relaxed), 1);
        }
        assert_eq!(x.load(Ordering::Relaxed), 1);
    }
}
