use crate::compare::Comparator;
use crate::opts::Opts;
use crate::utils::call_on_drop::CallOnDrop;
use crate::{call_on_drop, LError};
use bytes::{BufMut, Bytes, BytesMut};
use file_lock::{FileLock, FileOptions};
use std::cmp::min;
use std::collections::HashMap;
use std::env::current_dir;
use std::fs::{File, OpenOptions};
use std::io::SeekFrom;
use std::os::unix::fs::MetadataExt;

use lazy_static::lazy_static;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(crate) trait Encoding: Sized {
    fn encode<T: Comparator>(&self, dst: &mut BytesMut, opts: &Opts<T>) -> usize;
    fn decode<T: Comparator>(src: &mut Bytes, opts: &Opts<T>) -> Result<Self, LError>;
}

impl Encoding for Bytes {
    fn encode<C: Comparator>(&self, dst: &mut BytesMut, _opts: &Opts<C>) -> usize {
        dst.put_slice(self.as_ref());
        self.len()
    }

    fn decode<C: Comparator>(src: &mut Bytes, _opts: &Opts<C>) -> Result<Self, LError> {
        Ok(src.split_to(src.len()))
    }
}

pub trait Storage: Send + Sync + 'static {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, LError>;

    fn seek(&mut self, offset: u64) -> Result<(), LError>;

    fn size(&self) -> Result<u64, LError> {
        Ok(0)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), LError> {
        let mut start = 0;
        while start < buf.len() {
            let s = self.read(buf[start..].as_mut())?;
            start += s;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), LError> {
        Ok(())
    }

    fn write(&mut self, data: &[u8]) -> Result<usize, LError>;

    fn write_all(&mut self, data: &[u8]) -> Result<(), LError> {
        let mut start = 0;
        while start < data.len() {
            let s = self.write(data[start..].as_ref())?;
            start += s;
        }
        Ok(())
    }
}

impl Storage for File {
    fn seek(&mut self, offset: u64) -> Result<(), LError> {
        std::io::Seek::seek(self, SeekFrom::Start(offset))?;
        Ok(())
    }

    fn size(&self) -> Result<u64, LError> {
        Ok(self.metadata()?.size())
    }

    fn write_all(&mut self, data: &[u8]) -> Result<(), LError> {
        std::io::Write::write_all(self, data).map_err(LError::from)
    }

    fn flush(&mut self) -> Result<(), LError> {
        std::io::Write::flush(self).map_err(LError::from)
    }

    fn read(&mut self, buf: &mut [u8]) -> Result<usize, LError> {
        std::io::Read::read(self, buf).map_err(LError::from)
    }

    fn write(&mut self, data: &[u8]) -> Result<usize, LError> {
        std::io::Write::write(self, data).map_err(LError::from)
    }
}

// only used for testing
pub(crate) type MemFile = Arc<Mutex<(Vec<u8>, usize)>>;
impl Storage for MemFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, LError> {
        let mut f = self.lock()?;
        let len = min(f.0.len() - f.1, buf.len());
        (0..len).for_each(|i| {
            buf[i] = f.0[f.1];
            f.1 += 1;
        });
        Ok(len)
    }

    fn write(&mut self, data: &[u8]) -> Result<usize, LError> {
        let mut f = self.lock()?;
        f.0.extend_from_slice(data);
        Ok(data.len())
    }

    fn size(&self) -> Result<u64, LError> {
        Ok(self.lock().unwrap().0.len() as u64)
    }

    fn seek(&mut self, offset: u64) -> Result<(), LError> {
        let mut f = self.lock()?;
        f.1 = offset as usize;
        Ok(())
    }
}

// TODO: should use AsRef<Path> instead of &str
pub trait StorageSystem: Send + Sync + 'static {
    type O;
    fn open(&self, name: &str) -> Result<Self::O, LError>;

    fn pwd(&self) -> Result<String, LError>;

    fn create(&self, name: &str) -> Result<Self::O, LError>;

    fn remove(&self, name: &str) -> Result<(), LError>;

    fn rename(&self, old: &str, new: &str) -> Result<(), LError>;

    fn list(&self, dirname: &str) -> Result<Vec<String>, LError>;

    fn exists(&self, name: &str) -> Result<bool, LError>;

    fn mkdir_all(&self, name: &str) -> Result<(), LError>;

    fn lock(&self, name: &str) -> Result<Box<dyn Send>, LError>;
}

pub static OS_FS: OsFS = OsFS {};

#[derive(Default, Copy, Clone)]
pub struct OsFS {}

impl StorageSystem for OsFS {
    type O = File;

    fn open(&self, name: &str) -> Result<File, LError> {
        File::open(name).map_err(LError::from)
    }

    fn pwd(&self) -> Result<String, LError> {
        current_dir()?
            .to_str()
            .map(|x| x.to_string())
            .ok_or(LError::UnsupportedSystem(format!("pwd unsupported")))
    }

    fn mkdir_all(&self, name: &str) -> Result<(), LError> {
        std::fs::DirBuilder::new().recursive(true).create(name)?;
        Ok(())
    }

    fn create(&self, name: &str) -> Result<File, LError> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(name)
            .map_err(LError::from)
    }

    fn remove(&self, name: &str) -> Result<(), LError> {
        std::fs::remove_file(name).map_err(LError::from)
    }

    fn rename(&self, old: &str, new: &str) -> Result<(), LError> {
        std::fs::rename(old, new).map_err(LError::from)
    }

    fn list(&self, dirname: &str) -> Result<Vec<String>, LError> {
        let mut fns = vec![];
        for x in std::fs::read_dir(dirname)? {
            match x?.file_name().into_string() {
                Ok(s) => fns.push(s),
                Err(_) => {
                    return Err(LError::UnsupportedSystem(format!(
                        "file name should be a valid utf8 encoded string"
                    )))
                }
            }
        }
        Ok(fns)
    }

    fn exists(&self, name: &str) -> Result<bool, LError> {
        match std::fs::metadata(name) {
            Ok(_) => Ok(true),
            Err(k) => {
                if k.kind() == std::io::ErrorKind::NotFound {
                    Ok(false)
                } else {
                    Err(k.into())
                }
            }
        }
    }

    fn lock(&self, name: &str) -> Result<Box<dyn Send>, LError> {
        let opts = FileOptions::new().read(true).write(true).create(true);
        let l = FileLock::lock(name, true, opts)?;
        Ok(Box::new(l))
    }
}

lazy_static! {
    pub(crate) static ref MEM_FS: MemFS = MemFS::default();
}

#[derive(Clone, Default)]
pub(crate) struct MemFS {
    files: Arc<Mutex<HashMap<String, MemFile>>>,
}

impl StorageSystem for MemFS {
    type O = MemFile;

    fn open(&self, name: &str) -> Result<Self::O, LError> {
        self.files
            .lock()
            .unwrap()
            .get(name)
            .map(|x| x.clone())
            .ok_or(LError::Internal("not found".to_string()))
    }

    fn pwd(&self) -> Result<String, LError> {
        Ok(".".to_string())
    }

    fn mkdir_all(&self, name: &str) -> Result<(), LError> {
        let mut f = self.files.lock()?;
        f.entry(name.to_string())
            .or_insert(MemFile::new(Mutex::new((vec![], 0))));
        Ok(())
    }

    fn create(&self, name: &str) -> Result<Self::O, LError> {
        let f = Arc::new(Mutex::new((vec![], 0)));
        self.files
            .lock()
            .unwrap()
            .insert(name.to_string(), f.clone());
        Ok(f)
    }

    fn remove(&self, name: &str) -> Result<(), LError> {
        self.files.lock().unwrap().remove(name);
        Ok(())
    }

    fn rename(&self, old: &str, new: &str) -> Result<(), LError> {
        let mut files = self.files.lock().unwrap();
        if let Some(v) = files.remove(old) {
            files.insert(new.to_string(), v);
        }
        Ok(())
    }

    fn list(&self, _dirname: &str) -> Result<Vec<String>, LError> {
        Ok(self
            .files
            .lock()
            .unwrap()
            .keys()
            .map(|x| x.clone())
            .collect())
    }

    fn exists(&self, name: &str) -> Result<bool, LError> {
        let files = self.files.lock()?;
        Ok(files.contains_key(name))
    }

    fn lock(&self, name: &str) -> Result<Box<dyn Send>, LError> {
        loop {
            let mut files = self.files.lock()?;
            match files.get(name) {
                None => {
                    let f = MemFile::new(Mutex::new((vec![], 0)));
                    let fs = self.clone();
                    files.insert(name.to_string(), f);
                    let filename = name.to_string();
                    return Ok(Box::new(call_on_drop!({
                        let _ = fs.remove(filename.as_str());
                    })));
                }
                Some(_) => {
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
        }
    }
}
