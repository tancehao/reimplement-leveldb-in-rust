use crate::batch::{Batch, BatchWriter};
use crate::compare::{Comparator, ComparatorImpl};
use crate::filename::{db_filename, parse_dbname, set_current_file, FileType};
use crate::io::{Encoding, Storage, StorageSystem};
use crate::key::{InternalKey, InternalKeyComparator, InternalKeyRef};
use crate::memtable::MemTable;
use crate::metric::{Metric, COSTS};
use crate::opts::{Opts, ReadOptions, WriteOptions};
use crate::utils::call_on_drop::CallOnDrop;
use crate::version::{VersionEdit, VersionSet, LEVELS};
use crate::wal::{WalReader, WalWriter};
use crate::{call_on_drop, unregister, LError, L0_SLOWDOWN_WRITES_TRIGGER, L0_STOP_WRITES_TRIGGER};
use bytes::{Bytes, BytesMut};
use crossbeam::channel::{unbounded, Receiver, Sender};
use crossbeam::queue::SegQueue;
use std::cmp::{max, Ordering};
use std::collections::btree_set::BTreeSet;
use std::collections::vec_deque::VecDeque;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

pub struct DB<S: Storage, M: MemTable> {
    pub(crate) dirname: String,
    pub(crate) opts: Opts,
    pub(crate) ucmp: ComparatorImpl,
    pub(crate) icmp: InternalKeyComparator,
    pub(crate) inner: Arc<Mutex<DBInner<S, M>>>,
    pub(crate) write_tx: Arc<SegQueue<BatchWriter>>,
    pub(crate) write_rx: Arc<Mutex<(Option<BatchWriter>, Arc<SegQueue<BatchWriter>>)>>,
    #[allow(unused)]
    pub(crate) file_lock: Arc<Mutex<Box<dyn Send>>>,
    pub(crate) compacting_imem: Arc<(Mutex<bool>, Condvar)>,
    pub(crate) bgwork_trigger: Sender<BGWorkTask>,
    pub(crate) metric: Metric,
}

#[derive(Clone, Debug)]
pub(crate) enum BGWorkTask {
    Compaction,
    TryCleanSSTs(Vec<u64>),
    CleanWAL(u64),
}

impl<S: Storage, M: MemTable> Clone for DB<S, M> {
    fn clone(&self) -> Self {
        Self {
            dirname: self.dirname.clone(),
            opts: self.opts.clone(),
            ucmp: self.ucmp.clone(),
            icmp: self.icmp.clone(),
            inner: self.inner.clone(),
            write_tx: self.write_tx.clone(),
            write_rx: self.write_rx.clone(),
            file_lock: Arc::new(Mutex::new(Box::new(vec![0]))),
            compacting_imem: self.compacting_imem.clone(),
            bgwork_trigger: self.bgwork_trigger.clone(),
            metric: Metric::default(),
        }
    }
}

pub(crate) struct MemDB<M: MemTable, S: Storage> {
    mem: M,
    file_num: u64,
    wal: Option<Arc<Mutex<(WalWriter<S>, i64)>>>,
    batch_buf: VecDeque<BatchWriter>,
}

impl<M: MemTable, S: Storage> Deref for MemDB<M, S> {
    type Target = M;

    fn deref(&self) -> &Self::Target {
        &self.mem
    }
}

impl<M: MemTable, S: Storage> MemDB<M, S> {
    pub(crate) fn wal_log_num(&self) -> u64 {
        self.file_num
    }

    fn catch_up_with_wal(&mut self, _ctx: &mut Context) -> Result<(), LError> {
        while let Some(bwp) = self.batch_buf.pop_front() {
            for (seq_num, k, v) in bwp.batch().iter() {
                self.mem.set(k.clone(), seq_num, v.cloned());
            }
            bwp.notify_done();
        }
        Ok(())
    }
}

pub(crate) struct DBInner<S: Storage, M: MemTable> {
    pub(crate) versions: VersionSet<S>,
    pub(crate) memdb: MemDB<M, S>,
    pub(crate) imem: Option<Arc<MemDB<M, S>>>,
    pub(crate) pending_outputs: HashSet<u64>,
    // pub(crate) snapshots: SnapshotSets,
    pub(crate) snapshots: (BTreeSet<u64>, Arc<SegQueue<u64>>),
}

impl<S: Storage, M: MemTable> DB<S, M> {
    pub fn open(
        dirname: String,
        opts: Opts,
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
        let write_queue = Arc::new(SegQueue::new());
        let snapshot_release_chan = Arc::new(SegQueue::new());
        let metric = Metric::default();
        let db = Self {
            dirname: dirname.clone(),
            opts: opts.clone(),
            ucmp: opts.get_ucmp(),
            icmp: InternalKeyComparator::from(opts.get_ucmp()),
            compacting_imem: Arc::new((Mutex::new(false), Condvar::new())),
            inner: Arc::new(Mutex::new(DBInner {
                versions: VersionSet::new(
                    dirname.clone(),
                    bgwork_tx.clone(),
                    opts.clone(),
                    fs.clone(),
                    metric.clone(),
                ),
                memdb: MemDB {
                    mem: M::empty(),
                    file_num: 0,
                    wal: None,
                    batch_buf: Default::default(),
                },
                imem: None,
                pending_outputs: Default::default(),
                snapshots: (BTreeSet::new(), snapshot_release_chan.clone()),
                // snapshots: SnapshotSets::new(snapshot_release_chan.clone()),
            })),
            write_tx: write_queue.clone(),
            write_rx: Arc::new(Mutex::new((None, write_queue))),
            file_lock: Arc::new(Mutex::new(file_lock)),
            bgwork_trigger: bgwork_tx.clone(),
            metric,
        };

        let mut inner = db.inner.lock()?;
        inner.versions = VersionSet::new(
            dirname.clone(),
            bgwork_tx.clone(),
            opts.clone(),
            fs.clone(),
            db.metric.clone(),
        );

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
        inner.memdb.wal = Some(Arc::new(Mutex::new((
            WalWriter::new(fs.create(wal_name.as_str())?),
            -1,
        ))));
        drop(inner);
        if opts.get_enable_metrics_server() {
            db.start_metric_server();
        }
        db.start_bgwork(bgwork_rx);
        Ok(db)
    }

    fn replay_wal(&self, mut reader: WalReader<S>, opts: &Opts) -> Result<u64, LError> {
        let mut inner = self.inner.lock()?;
        let mut max_seq_num = 0;
        while let Some(log) = reader.next()? {
            let mut d = Bytes::from(log);
            while !d.is_empty() {
                let batch = Batch::decode(&mut d, &opts)?;
                for (seq_num, k, v) in batch.iter() {
                    inner.memdb.mem.set(k.clone(), seq_num, v.cloned());
                    max_seq_num = max(max_seq_num, seq_num);
                }
            }
        }
        drop(inner);
        self.compact()?;
        Ok(max_seq_num)
    }

    fn start_metric_server(&self) {
        std::thread::spawn(|| {
            let binding = "127.0.0.1:9184".parse().unwrap();
            prometheus_exporter::start(binding).unwrap();
        });
    }

    fn start_bgwork(&self, rx: Receiver<BGWorkTask>) {
        let handler = |db: &DB<S, M>, t: BGWorkTask| -> Result<(), LError> {
            match t {
                BGWorkTask::Compaction => {
                    db.compact()?;
                }
                BGWorkTask::TryCleanSSTs(nums) => {
                    let live_files = db.inner.lock()?.versions.current_version().all_file_nums();
                    let nums = nums.iter().filter(|x| live_files.get(x).is_none());
                    for num in nums {
                        let table_name = db_filename(db.dirname.as_str(), FileType::Table(*num));
                        let fs = db.inner.lock()?.versions.fs.clone();
                        let _ = fs.remove(table_name.as_str());
                        if fs.remove(table_name.as_str()).is_ok() {
                            db.metric.decr_sst_files();
                        }
                    }
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
                        if fs.remove(wal.as_ref()).is_ok() {
                            db.metric.decr_wal_files();
                        }
                    }
                }
            }
            Ok(())
        };
        let db = self.clone();
        let metric = self.metric.clone();
        std::thread::spawn(move || {
            metric.incr_threads_cnt();
            for t in rx {
                if let Err(e) = handler(&db, t) {
                    println!("error occurred when handling bg works: {:?}", e);
                }
            }
            metric.decr_threads_cnt();
        });
    }

    fn make_room_for_write(&self, ctx: &mut Context, mut force: bool) -> Result<(), LError> {
        ctx.start_trace("make_room");
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
                self.wait_imem_compaction_finish();
                continue;
            }
            if inner.versions.current_version().file_nums_in_level(0) > L0_STOP_WRITES_TRIGGER {
                drop(inner);
                self.wait_imem_compaction_finish();
                continue;
            }
            ctx.start_trace("change_wal");

            let new_log_number = inner.versions.incr_file_number();
            let new_wal_file = inner.versions.fs.create(
                db_filename(self.dirname.as_str(), FileType::WAL(new_log_number)).as_str(),
            )?;
            let new_wal_writer = WalWriter::new(new_wal_file);
            let mut new_memdb = MemDB {
                mem: M::empty(),
                file_num: new_log_number,
                wal: Some(Arc::new(Mutex::new((new_wal_writer, -1)))),
                batch_buf: VecDeque::new(),
            };
            for snapshot in inner.snapshots.0.iter() {
                new_memdb.mem.snapshot_acquired(*snapshot);
            }
            // somebody may be holding a pointer of the wal and writing logs into it, and we cannot
            // replace the memdb now otherwise the logs are written to an old WAL while inserted into
            // a new mem store. So we must hold a lock before doing the replace
            let mut old_memdb = std::mem::replace(&mut inner.memdb, new_memdb);
            if let Some(wal) = old_memdb.wal.as_ref() {
                let wal_seq_num = wal.lock()?.1;
                if wal_seq_num > 0 && old_memdb.mem.last_seq_num() < wal_seq_num as u64 {
                    old_memdb.catch_up_with_wal(ctx)?;
                }
            }
            self.metric.incr_wal_files();
            inner.imem = Some(Arc::new(old_memdb));
            inner.versions.log_number = new_log_number;
            force = false;
            ctx.end_trace("change_wal");
            self.maybe_schedule_compaction();
        }
        ctx.end_trace("make_room");
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
        let new_version = inner.versions.current_version().edit(ve, self.icmp);

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
impl<O: Storage, M: MemTable> DB<O, M> {
    pub fn get(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Bytes>, LError> {
        let (ikey, version) = {
            let l = self.inner.lock()?;
            let snapshot = match opts.snapshot() {
                Some(o) => o.seq_num(),
                None => l.versions.last_sequence(),
            };

            if let Some(Some(v)) = l.memdb.get(key, snapshot) {
                return Ok(Some(v));
            }
            if let Some(im) = &l.imem {
                if let Some(Some(v)) = im.get(key, snapshot) {
                    return Ok(Some(v));
                }
            }
            (
                InternalKeyRef::from((key, snapshot)).to_owned(),
                l.versions.current_version_cloned(),
            )
        };

        if let Some((ik, val)) = version.get(ikey.clone(), &self.opts)? {
            if self.opts.get_ucmp().compare(ik.ukey(), ikey.ukey()) == Ordering::Equal {
                if ik.is_set() {
                    return Ok(Some(val));
                }
            }
        }

        Ok(None)
    }

    pub fn del(&self, opts: &WriteOptions, key: Bytes) -> Result<(), LError> {
        let mut batch = Batch::default();
        batch.delete(key);
        self.apply_inner(opts, batch)
    }

    pub fn set(&self, opts: &WriteOptions, key: Bytes, value: Bytes) -> Result<(), LError> {
        let mut batch = Batch::default();
        batch.set(key, value);
        self.apply_inner(opts, batch)
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
        writeln!(info, "num={}", inner.memdb.wal_log_num())?;

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
        writeln!(info, "[System]")?;
        // TODO
        writeln!(info, "threads={}", self.metric.threads.load(Relaxed))?;
        writeln!(info, "memory={}", 0)?;
        Ok(info)
    }

    pub fn apply(&self, opts: &WriteOptions, batch: Batch) -> Result<(), LError> {
        self.apply_inner(opts, batch)
    }

    pub(crate) fn apply_inner(&self, opts: &WriteOptions, batch: Batch) -> Result<(), LError> {
        let mut ctx = Context::default();
        self.make_room_for_write(&mut ctx, false)?;

        let batch_writer = BatchWriter::new(batch);
        let notifier = batch_writer.notifier();
        self.write_tx.push(batch_writer);
        loop {
            match self.write_rx.try_lock() {
                Err(_) => {
                    let mut done = notifier.0.lock().unwrap();
                    if *done {
                        return Ok(());
                    }
                    done = notifier.1.wait(done).unwrap();
                    if *done {
                        return Ok(());
                    }
                }
                Ok(mut rx) => {
                    let mut log = BytesMut::new();
                    let mut bws = VecDeque::new();
                    let mut inner = self.inner.lock()?;
                    if let Some(mut bw) = rx.0.take() {
                        bw.set_seq_num(inner.versions.last_sequence());
                        inner
                            .versions
                            .advance_last_sequence(bw.batch().count() as u64);
                        bw.write_batch(&mut log, &self.opts);
                        bws.push_back(bw);
                    }
                    while let Some(mut bw) = rx.1.pop() {
                        bw.set_seq_num(inner.versions.last_sequence());
                        inner
                            .versions
                            .advance_last_sequence(bw.batch().count() as u64);
                        bw.write_batch(&mut log, &self.opts);
                        bws.push_back(bw);
                    }
                    let (memdb_num, wal) = (inner.memdb.file_num, inner.memdb.wal.clone());
                    drop(inner);
                    if let Some(v) = wal {
                        let mut wal = v.lock()?;
                        wal.0.append(log.as_ref())?;
                        let sync = opts.sync().unwrap_or(self.opts.get_flush_wal());
                        if sync {
                            wal.0.flush()?;
                        }
                    }
                    let mut inner = self.inner.lock()?;
                    if inner.memdb.file_num != memdb_num {
                        return Ok(());
                    }
                    {
                        while let Some(seq_num) = inner.snapshots.1.pop() {
                            inner.snapshots.0.remove(&seq_num);
                            inner.memdb.mem.snapshot_released(seq_num);
                        }
                    }
                    inner.memdb.batch_buf = bws;
                    inner.memdb.catch_up_with_wal(&mut ctx)?;
                    if let Some(bw) = rx.1.pop() {
                        bw.notify_done();
                        rx.0 = Some(bw);
                    }
                    return Ok(());
                }
            }
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let mut inner = self.inner.lock().unwrap();
        let seq_num = inner.versions.last_sequence;
        inner.snapshots.0.insert(seq_num);
        inner.memdb.mem.snapshot_acquired(seq_num);
        Snapshot {
            seq_num: seq_num,
            release_queue: inner.snapshots.1.clone(),
        }
    }

    #[allow(unused)]
    pub fn compact_manually(&self) -> Result<(), LError> {
        Err(LError::Internal("unimplemented".into()))
    }

    #[allow(unused)]
    pub fn dump<D: AsRef<Path>>(&self, target_dir: D) -> Result<(), LError> {
        Err(LError::Internal("unimplemented".into()))
    }
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    seq_num: u64,
    release_queue: Arc<SegQueue<u64>>,
}

impl Snapshot {
    pub fn seq_num(&self) -> u64 {
        self.seq_num
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.release_queue.push(self.seq_num);
    }
}

pub trait DBScanner {
    fn next(&mut self) -> Result<Option<(InternalKey, Bytes)>, LError>;
}

pub(crate) struct DedupScanner<'a> {
    scanner: Box<dyn DBScanner + 'a>,
    last: Option<InternalKey>,
    ucmp: ComparatorImpl,
}

impl<'a> DBScanner for DedupScanner<'a> {
    fn next(&mut self) -> Result<Option<(InternalKey, Bytes)>, LError> {
        loop {
            match self.scanner.next()? {
                None => return Ok(None),
                Some((ik, v)) => {
                    if let Some(k) = self.last.as_ref() {
                        if self.ucmp.compare(k.ukey(), ik.ukey()) == Ordering::Equal {
                            continue;
                        }
                    }
                    self.last = Some(ik.clone());
                    return Ok(Some((ik, v)));
                }
            }
        }
    }
}

impl<'a> DedupScanner<'a> {
    #[allow(unused)]
    pub(crate) fn new(scanner: Box<dyn DBScanner + 'a>, ucmp: ComparatorImpl) -> Self {
        Self {
            scanner,
            ucmp,
            last: None,
        }
    }
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

pub(crate) struct MergeScanner<'a> {
    scanners: Vec<(Box<dyn DBScanner + 'a>, Option<(InternalKey, Bytes)>)>,
    cmp: InternalKeyComparator,
}

impl<'a> DBScanner for MergeScanner<'a> {
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

impl<'a> MergeScanner<'a> {
    pub(crate) fn new(cmp: InternalKeyComparator, scanners: Vec<Box<dyn DBScanner + 'a>>) -> Self {
        Self {
            cmp,
            scanners: scanners.into_iter().map(|x| (x, None)).collect(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::db::DB;
    use crate::io::{MemFS, MemFile};
    use crate::key::InternalKeyRef;
    use crate::memtable::simple::BTMap;
    use crate::opts::{OptsRaw, ReadOptions, WriteOptions};
    use bytes::Bytes;
    use lazy_static::lazy_static;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_reopen_db() {
        let (ro, wo) = (ReadOptions::default(), WriteOptions::default());
        let mut opt_raw: OptsRaw = OptsRaw::default();
        opt_raw.write_buffer_size = 256;
        opt_raw.block_size = 512;
        opt_raw.max_file_size = 4096;
        opt_raw.flush_wal = true;
        let opts = Arc::new(opt_raw);
        lazy_static! {
            pub(crate) static ref TEST_MEM_FS1: MemFS = MemFS::default();
        }
        let fs = &*TEST_MEM_FS1;
        let db: DB<MemFile, BTMap> = DB::open("test_reopen".to_string(), opts.clone(), fs).unwrap();
        let mut kvs = HashMap::new();
        for i in 1..10000 {
            let uk = format!("key:{}", i);
            let value = format!("value:{}:{}:{}", i, i, i);
            let ik = InternalKeyRef::from((uk.as_ref(), i)).to_owned();
            if i % 20 == 0 {
                kvs.insert(ik, None);
                assert!(db.del(&wo, uk.into()).is_ok());
            } else {
                kvs.insert(ik, Some(value.clone()));
                assert!(db.set(&wo, uk.into(), value.into()).is_ok());
            }
        }
        drop(db);
        let db: DB<MemFile, BTMap> = DB::open("test_reopen".to_string(), opts.clone(), fs).unwrap();
        for (k, v) in kvs.iter() {
            let vv = v.clone().map(|x| Bytes::from(x));
            assert_eq!(db.get(&ro, k.ukey()).unwrap(), vv);
        }
    }

    #[test]
    fn test_concatenating_scanner() {}

    #[test]
    fn test_merging_scanner() {}
}

#[cfg(debug_assertions)]
pub(crate) type Context = ContextImpl;

#[cfg(not(debug_assertions))]
pub(crate) type Context = EmptyContext;

#[derive(Clone, Debug)]
pub struct ContextImpl {
    #[allow(unused)]
    thread_id: ThreadId,
    #[allow(unused)]
    trace: HashMap<&'static str, Instant>,
    metrics: HashMap<&'static str, u128>,
}

impl Default for ContextImpl {
    fn default() -> Self {
        Self {
            thread_id: std::thread::current().id(),
            trace: HashMap::new(),
            metrics: HashMap::new(),
        }
    }
}

impl ContextImpl {
    #[allow(unused)]
    pub(crate) fn start_trace(&mut self, name: &'static str) {
        self.trace.insert(name, Instant::now());
    }

    #[allow(unused)]
    pub(crate) fn end_trace(&mut self, name: &'static str) {
        match self.trace.get(name) {
            None => panic!("trace unstarted"),
            Some(v) => {
                let s = Instant::now().duration_since(*v).as_nanos();
                self.metrics.insert(name, s);
                self.trace.remove(name);
            }
        }
    }
}

impl Drop for ContextImpl {
    fn drop(&mut self) {
        for (m, c) in self.metrics.iter() {
            if let Some(hist) = COSTS.get(m) {
                hist.observe(*c as f64);
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct EmptyContext {}

impl EmptyContext {
    #[allow(unused)]
    pub(crate) fn start_trace(&mut self, _name: &'static str) {}

    #[allow(unused)]
    pub(crate) fn end_trace(&mut self, _name: &'static str) {}
}
