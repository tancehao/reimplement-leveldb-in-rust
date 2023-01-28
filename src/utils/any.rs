use std::any::TypeId;
use std::fmt::Debug;
use std::ptr::NonNull;
use std::{fmt, mem};

pub struct Any {
    data: NonNull<()>,
    vtable: &'static AnyVTable,
}

unsafe impl Send for Any {}
unsafe impl Sync for Any {}

struct AnyVTable {
    type_id: unsafe fn() -> TypeId,
    drop: unsafe fn(*mut ()),
    clone: unsafe fn(*const ()) -> Any,
}

impl AnyVTable {
    unsafe fn v_type_id<T: Send + Sync + 'static>() -> TypeId {
        TypeId::of::<T>()
    }

    unsafe fn v_drop<T: Send + Sync + 'static>(this: *mut ()) {
        drop(Box::from_raw(this.cast::<T>()))
    }

    unsafe fn v_clone<T: Clone + Send + Sync + 'static>(this: *const ()) -> Any {
        let x = Clone::clone(&*this.cast::<T>());
        Any::new(x)
    }
}

impl Any {
    pub fn new<T: Clone + Send + Sync + 'static>(x: T) -> Self {
        unsafe {
            Self {
                data: NonNull::new_unchecked(Box::into_raw(Box::new(x)).cast()),
                vtable: &AnyVTable {
                    type_id: AnyVTable::v_type_id::<T>,
                    drop: AnyVTable::v_drop::<T>,
                    clone: AnyVTable::v_clone::<T>,
                },
            }
        }
    }

    pub fn type_id(&self) -> TypeId {
        unsafe { (self.vtable.type_id)() }
    }

    pub fn downcast<T: Send + Sync + 'static>(self) -> Result<Box<T>, Self> {
        if self.type_id() == TypeId::of::<T>() {
            let ptr = self.data.as_ptr().cast::<T>();
            mem::forget(self);
            unsafe { Ok(Box::from_raw(ptr)) }
        } else {
            Err(self)
        }
    }

    pub fn downcast_ref<T: Send + Sync + 'static>(&self) -> Result<&T, &Self> {
        if self.type_id() == TypeId::of::<T>() {
            let ptr = self.data.as_ptr().cast::<T>();
            Ok(unsafe { &*ptr })
        } else {
            Err(self)
        }
    }
}

impl Clone for Any {
    fn clone(&self) -> Self {
        unsafe { (self.vtable.clone)(self.data.as_ptr()) }
    }
}

impl Drop for Any {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop)(self.data.as_ptr()) }
    }
}

impl Debug for Any {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Any {{ .. }}")
    }
}
