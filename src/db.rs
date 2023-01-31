use crate::batch::Batch;
use crate::compare::Comparator;
use crate::filename::{db_filename, parse_dbname, set_current_file, FileType};
use crate::io::{Encoding, Storage, StorageSystem};
use crate::key::{InternalKey, InternalKeyComparator, InternalKeyRef};
use crate::memtable::MemTable;
use crate::opts::Opts;
use crate::utils::call_on_drop::CallOnDrop;
use crate::version::{VersionEdit, VersionSet, LEVELS};
use crate::wal::{WalReader, WalWriter};
use crate::{call_on_drop, unregister, LError, L0_SLOWDOWN_WRITES_TRIGGER, L0_STOP_WRITES_TRIGGER};
use bytes::{Bytes, BytesMut};
use crossbeam::channel::{unbounded, Receiver, Sender};
use std::cmp::{max, Ordering};
use std::collections::{HashSet, VecDeque};
use std::ops::Deref;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

pub struct DB<C: Comparator, S: Storage, M: MemTable> {
    pub(crate) dirname: String,
    pub(crate) opts: Opts<C>,
    pub(crate) ucmp: C,
    pub(crate) icmp: InternalKeyComparator<C>,
    pub(crate) inner: Arc<Mutex<DBInner<C, S, M>>>,
    #[allow(unused)]
    pub(crate) file_lock: Arc<Mutex<Box<dyn Send>>>,
    pub(crate) compactioning: Arc<RwLock<()>>,
    pub(crate) bgwork_trigger: Sender<BGWorkTask>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum BGWorkTask {
    Compaction,
    CleanSST(u64),
    CleanWAL(u64),
}

impl<C: Comparator, S: Storage, M: MemTable> Clone for DB<C, S, M> {
    fn clone(&self) -> Self {
        Self {
            dirname: self.dirname.clone(),
            opts: self.opts.clone(),
            ucmp: self.ucmp.clone(),
            icmp: self.icmp.clone(),
            inner: self.inner.clone(),
            file_lock: Arc::new(Mutex::new(Box::new(vec![0]))),
            compactioning: self.compactioning.clone(),
            bgwork_trigger: self.bgwork_trigger.clone(),
        }
    }
}

pub(crate) struct MemDB<M: MemTable, S: Storage> {
    mem: M,
    wal: Option<Wal<S>>,
}

impl<M: MemTable, S: Storage> Deref for MemDB<M, S> {
    type Target = M;

    fn deref(&self) -> &Self::Target {
        &self.mem
    }
}

impl<M: MemTable, S: Storage> MemDB<M, S> {
    pub(crate) fn wal_log_num(&self) -> Option<u64> {
        self.wal.as_ref().map(|x| x.file_num)
    }
}

pub(crate) struct Wal<S: Storage> {
    writer: Arc<Mutex<WalWriter<S>>>,
    file_num: u64,
    buf: Arc<Mutex<VecDeque<Arc<Batch>>>>,
}

impl<S: Storage> Wal<S> {
    pub(crate) fn new(writer: Arc<Mutex<WalWriter<S>>>, file_num: u64) -> Self {
        Self {
            writer,
            file_num,
            buf: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

pub(crate) struct DBInner<C: Comparator, S: Storage, M: MemTable> {
    pub(crate) versions: VersionSet<C, S>,
    pub(crate) memdb: MemDB<M, S>,
    pub(crate) imem: Option<Arc<MemDB<M, S>>>,
    pub(crate) pending_outputs: HashSet<u64>,
}

impl<C: Comparator, S: Storage, M: MemTable> DB<C, S, M> {
    pub fn open(
        dirname: String,
        opts: Opts<C>,
        fs: &'static dyn StorageSystem<O = S>,
    ) -> Result<Self, LError> {
        let dirname = if dirname.starts_with("/") {
            dirname
        } else {
            format!("{}/{}", fs.pwd()?, dirname)
        };
        fs.mkdir_all(dirname.as_str())?;
        let file_lock = fs.lock(db_filename(dirname.as_str(), FileType::Lock).as_str())?;
        let (bgwork_tx, bgwork_rx) = unbounded();
        let db = Self {
            dirname: dirname.clone(),
            opts: opts.clone(),
            ucmp: opts.get_comparator(),
            icmp: InternalKeyComparator::from(opts.get_comparator()),
            compactioning: Arc::new(RwLock::new(())),
            inner: Arc::new(Mutex::new(DBInner {
                versions: VersionSet::new(
                    dirname.clone(),
                    bgwork_tx.clone(),
                    opts.clone(),
                    fs.clone(),
                ),
                memdb: MemDB {
                    mem: M::empty(),
                    wal: None,
                },
                imem: None,
                pending_outputs: Default::default(),
            })),
            file_lock: Arc::new(Mutex::new(file_lock)),
            bgwork_trigger: bgwork_tx.clone(),
        };

        let mut inner = db.inner.lock()?;
        inner.versions =
            VersionSet::new(dirname.clone(), bgwork_tx.clone(), opts.clone(), fs.clone());

        let current_name = db_filename(dirname.as_str(), FileType::Current);

        if !fs.exists(current_name.as_str())? {
            inner.versions.create_new()?;
        } else {
            if opts.get_error_if_db_exists() {
                return Err(LError::Internal(format!(
                    "the db {} already exists",
                    dirname
                )));
            }
            inner.versions.load(opts.clone())?;
        }

        let file_names = fs.list(dirname.as_str())?;
        let mut wals = vec![];
        for fname in file_names.iter() {
            if let Some(FileType::WAL(n)) = parse_dbname(fname.as_str()) {
                if n >= inner.versions.log_number {
                    let full_name = format!("{}/{}", dirname, fname);
                    wals.push((
                        WalReader::new(fs.open(full_name.as_str())?, fname.clone())?,
                        n,
                    ));
                }
            }
        }
        wals.sort_by(|(_, a), (_, b)| a.cmp(b));
        // replay the wal log
        for (wal, file_num) in wals {
            drop(inner);
            let max_seq_num = db.replay_wal(wal, &opts)?;
            inner = db.inner.lock()?;
            inner.versions.mark_file_num_used(file_num);
            if max_seq_num > inner.versions.last_sequence() {
                let step = max_seq_num - inner.versions.last_sequence();
                inner.versions.advance_last_sequence(step);
            }
        }
        let log_number = inner.versions.incr_file_number();
        let wal_name = db_filename(dirname.as_str(), FileType::WAL(log_number));
        let ww = Arc::new(Mutex::new(WalWriter::new(fs.create(wal_name.as_str())?)));
        inner.memdb.wal = Some(Wal::new(ww, log_number));
        drop(inner);
        db.start_bgwork(bgwork_rx);
        Ok(db)
    }

    fn replay_wal(&self, mut reader: WalReader<S>, opts: &Opts<C>) -> Result<u64, LError> {
        let mut inner = self.inner.lock()?;
        let mut max_seq_num = 0;
        while let Some(log) = reader.next()? {
            let mut d = Bytes::from(log);
            let batch = Batch::decode(&mut d, &opts)?;
            for (seq_num, k, v) in batch.iter() {
                match v {
                    Some(value) => inner.memdb.mem.set(k.clone(), seq_num, value.clone()),
                    None => inner.memdb.mem.del(k.clone(), seq_num),
                }
                max_seq_num = max(max_seq_num, seq_num);
            }
        }
        drop(inner);
        self.compact()?;
        Ok(max_seq_num)
    }

    fn start_bgwork(&self, rx: Receiver<BGWorkTask>) {
        let handler = |db: &DB<C, S, M>, t: BGWorkTask| -> Result<(), LError> {
            match t {
                BGWorkTask::Compaction => {
                    db.compact()?;
                }
                // FIXME should ensure the file is not contained in current version
                BGWorkTask::CleanSST(num) => {
                    let table_name = db_filename(db.dirname.as_str(), FileType::Table(num));
                    let fs = db.inner.lock()?.versions.fs.clone();
                    fs.remove(table_name.as_str())?;
                }
                BGWorkTask::CleanWAL(log_num) => {
                    let fs = db.inner.lock()?.versions.fs.clone();
                    let file_names = fs.list(db.dirname.as_str())?;
                    let wals = file_names
                        .into_iter()
                        .map(|x| match parse_dbname(x.as_str()) {
                            Some(FileType::WAL(num)) => {
                                if num < log_num {
                                    Some(x)
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        })
                        .filter(Option::is_some)
                        .map(Option::unwrap)
                        .map(|x| format!("{}/{}", db.dirname, x))
                        .collect::<Vec<String>>();
                    for wal in wals {
                        fs.remove(wal.as_ref())?;
                    }
                }
            }
            Ok(())
        };
        let db = self.clone();
        std::thread::spawn(move || {
            for t in rx {
                if let Err(e) = handler(&db, t) {
                    println!("error occurred when handling bg works: {:?}", e);
                }
            }
        });
    }

    fn make_room_for_write(&self, mut force: bool) -> Result<(), LError> {
        let mut allow_delay = !force;
        loop {
            let mut inner = self.inner.lock()?;
            if allow_delay
                && inner.versions.current_version().file_nums_in_level(0)
                    > L0_SLOWDOWN_WRITES_TRIGGER
            {
                drop(inner);
                std::thread::sleep(Duration::from_millis(1));
                allow_delay = false;
                continue;
            }
            if !force && inner.memdb.approximate_size() <= self.opts.get_write_buffer_size() {
                break;
            }
            if inner.imem.is_some() {
                drop(inner);
                let _l = self.compactioning.read()?;
                continue;
            }
            if inner.versions.current_version().file_nums_in_level(0) > L0_STOP_WRITES_TRIGGER {
                drop(inner);
                let _g = self.compactioning.read()?;
                continue;
            }
            let new_log_number = inner.versions.incr_file_number();
            let new_wal_file = inner.versions.fs.create(
                db_filename(self.dirname.as_str(), FileType::WAL(new_log_number)).as_str(),
            )?;
            let new_wal_writer = WalWriter::new(new_wal_file);
            let new_memdb = MemDB {
                mem: M::empty(),
                wal: Some(Wal::new(
                    Arc::new(Mutex::new(new_wal_writer)),
                    new_log_number,
                )),
            };
            inner.imem = Some(Arc::new(std::mem::replace(&mut inner.memdb, new_memdb)));
            inner.versions.log_number = new_log_number;
            force = false;
            self.maybe_schedule_compaction();
        }
        Ok(())
    }

    pub(crate) fn apply_version_edit(&self, ve: &mut VersionEdit<S>) -> Result<(), LError> {
        let inner = self.inner.lock()?;

        if ve.log_number != 0 {
            if ve.log_number < inner.versions.log_number
                || ve.log_number >= inner.versions.next_file_number
            {
                panic!("inconsistent VersionEdit log_number: {}", ve.log_number);
            }
        }
        if ve.log_number == 0 {
            ve.log_number = inner.versions.log_number;
        }
        ve.next_file_number = inner.versions.next_file_number;
        ve.last_sequence = inner.versions.last_sequence;
        let new_version = inner.versions.current_version().edit(ve);
        let manifest_number = inner.versions.manifest_file_number;
        let fs = inner.versions.fs.clone();
        let manifest_file = inner.versions.manifest_file.clone();
        let snapshot = inner.versions.make_snapshot();

        let mut mfile = manifest_file.lock()?;
        // we're going to write logs to the manifest file, release the lock now.
        drop(inner);

        let mut tmpfile_recycler = None;
        if mfile.is_none() {
            let filename = db_filename(self.dirname.as_str(), FileType::Manifest(manifest_number));
            let file = fs.create(filename.as_str())?;
            let fs_c = fs.clone();
            tmpfile_recycler = Some(call_on_drop!({
                let _ = fs_c.remove(filename.as_str());
            }));
            let mut ww = WalWriter::new(file);
            let mut log = BytesMut::new();
            snapshot.encode(&mut log, &self.opts);
            ww.append(log.as_ref())?;
            *mfile = Some(ww);
        }
        let ww = mfile.as_mut().unwrap();
        let mut log = BytesMut::new();
        ve.encode(&mut log, &self.opts);
        ww.append(log.as_ref())?;
        ww.flush()?;
        set_current_file(fs, self.dirname.as_str(), manifest_number)?;
        drop(mfile);
        let mut inner = self.inner.lock()?;
        inner.versions.set_version(Arc::new(new_version));
        if ve.log_number != 0 {
            inner.versions.log_number = ve.log_number;
        }
        tmpfile_recycler.map(|mut x| unregister!(x));
        Ok(())
    }
}

// exported methods
impl<C: Comparator, O: Storage, M: MemTable> DB<C, O, M> {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>, LError> {
        let (ikey, version) = {
            let l = self.inner.lock()?;
            let ikey = InternalKeyRef::from((key, l.versions.last_sequence()));
            if let Some(v) = l.memdb.get(key) {
                return Ok(Some(v));
            }
            if let Some(im) = &l.imem {
                if let Some(v) = im.get(key) {
                    return Ok(Some(v));
                }
            }
            (ikey, l.versions.current_version_cloned())
        };
        version.get(ikey, &self.opts)
    }

    pub fn del(&self, key: Bytes) -> Result<(), LError> {
        let mut batch = Batch::default();
        batch.delete(key);
        self.apply(batch)
    }

    pub fn set(&self, key: Bytes, value: Bytes) -> Result<(), LError> {
        let mut batch = Batch::default();
        batch.set(key, value);
        self.apply(batch)
    }

    // FIXME: should return a struct and it's the caller's responsibility to display
    pub fn info(&self) -> Result<String, LError> {
        use std::fmt::Write;
        let mut info = String::new();
        let inner = self.inner.lock()?;
        let version = inner.versions.current_version();
        writeln!(info, "\n[FILES]")?;
        for level in 0..LEVELS {
            let file_infos = version
                .files_in_level(level)
                .iter()
                .map(|x| format!("[{}]({}:{})", x.num, x.smallest, x.largest))
                .collect::<Vec<String>>();
            writeln!(info, "file_nums_in_level{}={}", level, file_infos.len())?;
            writeln!(info, "file_in_level{}={:?}", level, file_infos)?;
        }
        writeln!(info, "[MEM]")?;
        writeln!(info, "size={}", inner.memdb.approximate_size())?;
        writeln!(info, "len={}", inner.memdb.len())?;
        writeln!(info, "[IMEM]")?;
        writeln!(info, "exists={}", inner.imem.is_some())?;
        writeln!(info, "[WAL]")?;
        writeln!(info, "exists={}", inner.memdb.wal.is_some())?;
        if let Some(n) = inner.memdb.wal_log_num() {
            writeln!(info, "num={}", n)?;
        }
        writeln!(info, "[VERSIONS]")?;
        writeln!(info, "last_sequence={}", inner.versions.last_sequence())?;
        writeln!(
            info,
            "current_file_number:={}",
            inner.versions.current_file_number()
        )?;
        writeln!(info, "log_number={}", inner.versions.log_number)?;
        writeln!(info, "[BLOCK CACHE]")?;
        writeln!(info, "size={}", inner.versions.block_cache.lock()?.size())?;
        writeln!(info, "limit={}", inner.versions.block_cache.lock()?.limit())?;
        writeln!(info, "len={}", inner.versions.block_cache.lock()?.len())?;
        writeln!(info, "[PendingOutputs]")?;
        writeln!(info, "{:?}", inner.pending_outputs)?;
        Ok(info)
    }

    pub fn apply(&self, mut batch: Batch) -> Result<(), LError> {
        self.make_room_for_write(false)?;
        let mut inner = self.inner.lock()?;
        batch.set_seq_num(inner.versions.last_sequence() + 1);
        inner.versions.advance_last_sequence(batch.count() as u64);
        let batch_ptr = Arc::new(batch);
        // batch.encode(&mut log, &self.opts);

        // we're going to write wal and it may block.
        // release the lock and acquire it again later
        if let Some(wal) = inner.memdb.wal.as_mut() {
            let flush_wal = self.opts.get_flush_wal();
            wal.buf.lock()?.push_back(batch_ptr.clone());
            let (writer, buf) = (wal.writer.clone(), wal.buf.clone());
            // now we drop the `inner` lock and other threads having entered this method
            // may be able to acquire it and push their Batch to the buf queue. And the thread
            // who acquire the `writer` lock later is able to write all these buffered batches
            // to WAL file in a single write and flush.
            drop(inner);
            let mut ww = writer.lock()?;
            let mut buf = buf.lock()?;
            let mut log = BytesMut::new();
            while let Some(b) = buf.pop_front() {
                b.encode(&mut log, &self.opts);
            }
            ww.append(log.as_ref())?;
            if flush_wal {
                ww.flush()?;
            }
            drop(ww);
            drop(buf);
            inner = self.inner.lock()?;
        }

        for (seq_num, k, v) in batch_ptr.iter() {
            match v {
                Some(val) => inner.memdb.mem.set(k.clone(), seq_num, val.clone()),
                None => inner.memdb.mem.del(k.clone(), seq_num),
            }
        }
        Ok(())
    }

    #[allow(unused)]
    pub fn snapshot(&self) -> Result<Snapshot<C, O, M>, LError> {
        Err(LError::Internal("unimplemented".into()))
    }

    #[allow(unused)]
    pub fn compact_manually(&self) -> Result<(), LError> {
        Err(LError::Internal("unimplemented".into()))
    }

    #[allow(unused)]
    pub fn dump<D: AsRef<Path>>(&self, target_dir: D) -> Result<(), LError> {
        Err(LError::Internal("unimplemented".into()))
    }

    #[allow(unused)]
    pub fn watch_event(&self, event_type: EventType) -> Result<(), LError> {
        Err(LError::Internal("unimplemented".into()))
    }
}

#[derive(Copy, Clone, Debug)]
pub enum EventType {
    Opened,
    WalWrote,
    MemFreezed,
    ImemCompacted,
    SSTCompacted,
    NewSSTAdded,
    SSTRemoved,
    Closed,
}

#[allow(unused)]
pub struct Snapshot<C: Comparator, O: Storage, M: MemTable> {
    seq_num: u64,
    db: DB<C, O, M>,
}

pub trait DBScanner {
    fn next(&mut self) -> Result<Option<(InternalKey, Bytes)>, LError>;
}

pub(crate) struct ConcatScanner<'a> {
    scanners: VecDeque<Box<dyn DBScanner + 'a>>,
}

impl<'a> DBScanner for ConcatScanner<'a> {
    fn next(&mut self) -> Result<Option<(InternalKey, Bytes)>, LError> {
        while !self.scanners.is_empty() {
            match self.scanners[0].next()? {
                Some(kv) => return Ok(Some(kv)),
                None => {
                    self.scanners.pop_front();
                }
            }
        }
        Ok(None)
    }
}

impl<'a> From<Vec<Box<dyn DBScanner + 'a>>> for ConcatScanner<'a> {
    fn from(value: Vec<Box<dyn DBScanner + 'a>>) -> Self {
        Self {
            scanners: value.into(),
        }
    }
}

pub(crate) struct MergeScanner<'a, V: Comparator + 'a> {
    scanners: Vec<(Box<dyn DBScanner + 'a>, Option<(InternalKey, Bytes)>)>,
    cmp: V,
}

impl<'a, V: Comparator + 'a> DBScanner for MergeScanner<'a, V> {
    fn next(&mut self) -> Result<Option<(InternalKey, Bytes)>, LError> {
        if self.scanners.is_empty() {
            return Ok(None);
        }
        let mut finished = None;
        loop {
            for (i, (s, last)) in self.scanners.iter_mut().enumerate() {
                if last.is_none() {
                    match s.next()? {
                        Some(kv) => *last = Some(kv),
                        None => {
                            finished = Some(i);
                            break;
                        }
                    }
                }
            }
            match finished.take() {
                Some(i) => {
                    self.scanners.remove(i);
                }
                None => break,
            }
        }
        if self.scanners.is_empty() {
            return Ok(None);
        }
        let mut index = 0;
        for (i, (_, last)) in self.scanners.iter().enumerate() {
            let (k, _) = last.as_ref().unwrap();
            if self.cmp.compare(
                k.as_ref(),
                self.scanners[index].1.as_ref().unwrap().0.as_ref(),
            ) == Ordering::Less
            {
                index = i;
            }
        }

        Ok(self.scanners[index].1.take())
    }
}

impl<'a, C: Comparator> MergeScanner<'a, C> {
    pub(crate) fn new(cmp: C, scanners: Vec<Box<dyn DBScanner + 'a>>) -> Self {
        Self {
            cmp,
            scanners: scanners.into_iter().map(|x| (x, None)).collect(),
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_concatenating_scanner() {}

    #[test]
    fn test_merging_scanner() {}
}
