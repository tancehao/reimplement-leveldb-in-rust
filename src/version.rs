use crate::compare::{Comparator, ComparatorImpl};
use crate::db::BGWorkTask;
use crate::filename::{db_filename, set_current_file, FileType};
use crate::io::{Encoding, Storage, StorageSystem};
use crate::key::{InternalKey, InternalKeyComparator, InternalKeyRef};
use crate::opts::Opts;
use crate::sstable::reader::{BlockCache, SSTEntry, SSTableReader};
use crate::utils::varint::{put_uvarint, take_uvarint};
use crate::wal::{WalReader, WalWriter};
use crate::{LError, L0_COMPACTION_TRIGGER};
use bytes::{Bytes, BytesMut};
use crossbeam::channel::{unbounded, Receiver, Sender};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering::Relaxed;
use crate::sstable::metric::Metric;
use crate::utils::lru::LRUCache;

pub const LEVELS: usize = 7;

pub(crate) struct VersionSet<O: Storage> {
    dirname: String,
    pub(crate) opts: Opts,
    ucmp: ComparatorImpl,
    #[allow(unused)]
    icmp: InternalKeyComparator,
    version: VersionPtr<O>,
    #[allow(unused)]
    clean_trigger: Sender<BGWorkTask>,

    pub(crate) block_cache: BlockCache,

    pub(crate) log_number: u64,
    pub(crate) next_file_number: u64,
    pub(crate) last_sequence: u64,

    pub(crate) fs: &'static dyn StorageSystem<O = O>,

    pub(crate) manifest_file_number: u64,
    pub(crate) manifest_file: Arc<Mutex<Option<WalWriter<O>>>>,

    pub(crate) metric: Metric,
}

impl<O: Storage> VersionSet<O> {
    pub(crate) fn new(
        dirname: String,
        bgevent_trigger: Sender<BGWorkTask>,
        opts: Opts,
        fs: &'static dyn StorageSystem<O = O>,
        metric: Metric,
    ) -> Self {
        Self {
            dirname: dirname,
            ucmp: opts.get_comparator(),
            icmp: InternalKeyComparator::from(opts.get_comparator()),
            block_cache: LRUCache::new(opts.get_block_cache_limit()),
            opts: opts,
            version: Arc::new(Version::new(bgevent_trigger.clone())),
            log_number: 0,
            next_file_number: 0,
            last_sequence: 0,
            manifest_file_number: 0,
            manifest_file: Arc::new(Mutex::new(None)),
            clean_trigger: bgevent_trigger,
            fs,
            metric,
        }
    }

    #[inline]
    pub(crate) fn incr_file_number(&mut self) -> u64 {
        self.next_file_number += 1;
        self.next_file_number
    }

    #[inline]
    pub(crate) fn current_file_number(&self) -> u64 {
        self.next_file_number
    }

    #[inline]
    pub(crate) fn advance_last_sequence(&mut self, cnt: u64) -> u64 {
        self.last_sequence += cnt;
        self.last_sequence
    }

    #[inline]
    pub(crate) fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    pub(crate) fn mark_file_num_used(&mut self, num: u64) {
        if self.next_file_number < num {
            self.next_file_number = num;
        }
    }

    pub(crate) fn load(&mut self, opts: Opts) -> Result<(), LError> {
        let manifest_name = {
            let mut f = self
                .fs
                .open(db_filename(self.dirname.as_str(), FileType::Current).as_str())?;
            let size = f.size()?;
            if size == 0 {
                return Err(LError::InvalidFile(format!(
                    "CURRENT file for DB {} is empty",
                    self.dirname
                )));
            }
            if size > 4096 {
                return Err(LError::InvalidFile(format!(
                    "CURRENT file for DB {} is too large",
                    self.dirname
                )));
            }
            let mut buf = Vec::with_capacity(size as usize);
            buf.resize(size as usize, 0);
            f.read_exact(&mut buf)?;
            if buf.pop() != Some(b'\n') {
                return Err(LError::InvalidFile(format!(
                    "CURRENT file for DB {} is malformed",
                    self.dirname
                )));
            }
            String::from_utf8(buf).map_err(|_x| {
                LError::InvalidFile(format!("CURRENT file for DB {} is malformed", self.dirname))
            })?
        };
        let manifest_file = self
            .fs
            .open(format!("{}/{}", self.dirname, manifest_name).as_str())?;
        let mut ww = WalReader::new(manifest_file, manifest_name)?;
        let mut version = Version::default();
        let copy_if_meaningful = |dst: &mut u64, src: u64| {
            if src != 0 {
                *dst = src;
            }
        };
        loop {
            let mut log = match ww.next()? {
                Some(l) => Bytes::from(l),
                None => break,
            };
            let ve = VersionEdit::decode(&mut log, &opts)?;
            version = version.edit(&ve, self.icmp);
            copy_if_meaningful(&mut self.log_number, ve.log_number);
            copy_if_meaningful(&mut self.next_file_number, ve.next_file_number);
            copy_if_meaningful(&mut self.last_sequence, ve.last_sequence);
            if !ve.comparator_name.is_empty() {
                if ve.comparator_name.as_str() != self.ucmp.name() {
                    return Err(LError::InvalidFile(format!(
                        "comparator in manifest is {} while in Options it's {}",
                        ve.comparator_name,
                        self.ucmp.name()
                    )));
                }
            }
        }

        if self.next_file_number <= self.log_number {
            self.next_file_number = self.log_number + 1;
        }
        self.manifest_file_number = self.next_file_number;

        for files in version.files.iter_mut() {
            let is_leveled = files.is_leveled();
            for file in files.as_mut() {
                if file.file.is_none() {
                    let table_name = db_filename(self.dirname.as_str(), FileType::Table(file.num));
                    let table = self.fs.open(table_name.as_str())?;
                    let mut reader = SSTableReader::open(table, file.num, &self.opts)?;
                    reader.set_cache_handle(self.block_cache.clone());

                    let mut opened_file = DataFile {
                        num: file.num,
                        size: file.size,
                        smallest: file.smallest.clone(),
                        largest: file.largest.clone(),
                        file: Some(reader),
                        key_stream: None,
                    };
                    let dfp = if !is_leveled {
                        let (tx, rx) = unbounded();
                        opened_file.key_stream = Some(tx);
                        let opened_file = DataFilePtr::new(opened_file);
                        let file = opened_file.file.clone();
                        let metric = self.metric.clone();
                        let _ = std::thread::spawn(move || {
                            metric.threads.fetch_add(1, Relaxed);
                            DataFile::serve_queries(file, rx);
                            metric.threads.fetch_sub(1, Relaxed);
                        });
                        opened_file
                    } else {
                        DataFilePtr::new(opened_file)
                    };
                    *file = dfp;
                }
            }
        }
        self.set_version(Arc::new(version));
        Ok(())
    }

    pub(crate) fn make_snapshot(&self) -> VersionEdit<O> {
        let mut ve = VersionEdit::empty();
        for (level, files) in self.current_version().files.iter().enumerate() {
            files
                .as_ref()
                .iter()
                .for_each(|x| ve.new_file(level, x.clone()));
        }
        ve.comparator_name = self.ucmp.name().to_string();
        ve
    }

    pub(crate) fn set_version(&mut self, v: VersionPtr<O>) {
        self.version = v;
    }

    pub(crate) fn current_version(&self) -> &Version<O> {
        self.version.as_ref()
    }

    pub(crate) fn current_version_cloned(&self) -> VersionPtr<O> {
        self.version.clone()
    }

    pub(crate) fn create_new(&mut self) -> Result<(), LError> {
        const MANIFEST_FILE_NUM: u64 = 1;
        self.mark_file_num_used(MANIFEST_FILE_NUM + 1);
        let mn = db_filename(self.dirname.as_str(), FileType::Manifest(MANIFEST_FILE_NUM));
        let manifest_file = self.fs.create(mn.as_str())?;
        let mut manifest_writer = WalWriter::new(manifest_file);
        let mut buf = BytesMut::new();
        let mut ve: VersionEdit<O> = VersionEdit::empty();
        ve.set_comparator_name(self.opts.get_comparator().name().to_string());
        ve.set_next_file_number(MANIFEST_FILE_NUM + 1);
        ve.encode(&mut buf, &self.opts);
        manifest_writer.append(buf.as_ref())?;
        manifest_writer.flush()?;
        set_current_file(self.fs, self.dirname.as_str(), MANIFEST_FILE_NUM)
    }
}

pub(crate) type DataFilePtr<S> = Arc<DataFile<S>>;
pub(crate) type AsyncQuery = (Bytes, Sender<Result<Option<SSTEntry>, LError>>, Opts);

#[derive(Clone)]
pub(crate) struct DataFile<S: Storage> {
    pub(crate) num: u64,
    pub(crate) size: u64,
    pub(crate) smallest: InternalKey,
    pub(crate) largest: InternalKey,
    pub(crate) file: Option<SSTableReader<S>>,
    pub(crate) key_stream: Option<Sender<AsyncQuery>>,
}

impl<S: Storage> DataFile<S> {
    pub(crate) fn may_contains(&self, ukey: &[u8], ucmp: ComparatorImpl) -> bool {
        let r1 = ucmp.compare(ukey, self.smallest.ukey()) == Ordering::Less;
        let r2 = ucmp.compare(ukey, self.largest.ukey()) == Ordering::Greater;
        !(r1 || r2)
    }

    pub(crate) fn search(&self, ukey: &[u8], opts: &Opts) -> Result<Option<SSTEntry>, LError> {
        match self.file.as_ref() {
            None => Err(LError::Internal("db file is not opened".to_string())),
            Some(f) => f.get(ukey, opts),
        }
    }

    pub(crate) fn search_parallel(
        &self,
        key: Bytes,
        value_tx: Sender<Result<Option<SSTEntry>, LError>>,
        opts: Opts,
    ) -> Result<(), LError> {
        match self.key_stream.as_ref() {
            Some(s) => s.send((key, value_tx, opts)).map_err(|_| {
                LError::Internal(format!("thread for reading sst {} is broken", self.num))
            })?,
            None => {
                return Err(LError::Internal(
                    "sst file in tiered mode has no associated channels".to_string(),
                ))
            }
        }
        Ok(())
    }

    pub(crate) fn serve_queries(
        file: Option<SSTableReader<S>>,
        key_rx: Receiver<(Bytes, Sender<Result<Option<SSTEntry>, LError>>, Opts)>,
    ) {
        for (k, value_tx, opts) in key_rx {
            let res = match file.as_ref() {
                None => Err(LError::Internal("db file is not opened".to_string())),
                Some(f) => f.get(k.as_ref(), &opts),
            };
            let _ = value_tx.send(res);
        }
    }
}

impl<S: Storage> Debug for DataFile<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFile")
            .field("num", &self.num)
            .field("size", &self.size)
            .field("smallest", &self.smallest)
            .field("largest", &self.largest)
            .finish()
    }
}

pub(crate) fn ikey_range<S: Storage>(
    icmp: InternalKeyComparator,
    f0: &[DataFilePtr<S>],
    f1: Option<&[DataFilePtr<S>]>,
) -> (InternalKey, InternalKey) {
    if f0.is_empty() {
        return (InternalKey::default(), InternalKey::default());
    }
    let (mut smallest, mut largest) = (f0[0].smallest.clone(), f0[0].largest.clone());
    let mut c = |files: &[DataFilePtr<S>]| {
        for f in files.iter() {
            if icmp.compare(f.smallest.as_ref(), smallest.as_ref()) == Ordering::Less {
                smallest = f.smallest.clone();
            }
            if icmp.compare(f.largest.as_ref(), largest.as_ref()) == Ordering::Greater {
                largest = f.largest.clone();
            }
        }
    };
    c(f0);
    if let Some(f1) = f1 {
        c(f1);
    }
    (smallest, largest)
}

#[derive(Debug)]
pub(crate) enum LevelMode<S: Storage> {
    // sstables in tiered levels may have overlaps
    Tiered(Vec<DataFilePtr<S>>),
    // sstables in leveled levels form a single run
    Leveled(Vec<DataFilePtr<S>>),
}

impl<S: Storage> Default for LevelMode<S> {
    fn default() -> Self {
        LevelMode::Leveled(vec![])
    }
}

impl<S: Storage> AsRef<Vec<DataFilePtr<S>>> for LevelMode<S> {
    fn as_ref(&self) -> &Vec<DataFilePtr<S>> {
        match self {
            LevelMode::Leveled(files) => files,
            LevelMode::Tiered(files) => files,
        }
    }
}

impl<S: Storage> AsMut<Vec<DataFilePtr<S>>> for LevelMode<S> {
    fn as_mut(&mut self) -> &mut Vec<DataFilePtr<S>> {
        match self {
            LevelMode::Leveled(files) => files,
            LevelMode::Tiered(files) => files,
        }
    }
}

impl<S: Storage> LevelMode<S> {
    pub(crate) fn is_leveled(&self) -> bool {
        match self {
            LevelMode::Leveled(_) => true,
            LevelMode::Tiered(_) => false,
        }
    }

    pub(crate) fn search_in_level(&self, ikey: &InternalKeyRef, opts: &Opts) -> Result<Option<SSTEntry>, LError> {
        let ukey = ikey.ukey;

        let r = match self {
            LevelMode::Tiered(files) => {
                if opts.get_tiered_parallel() {
                    let (value_tx, value_rx) = crossbeam::channel::bounded(files.len());
                    for file in files.iter().rev() {
                        if file.may_contains(ikey.ukey, opts.get_comparator()) {
                            file.search_parallel(
                                ukey.to_vec().into(),
                                value_tx.clone(),
                                opts.clone(),
                            )?;
                        }
                    }
                    drop(value_tx);
                    let mut result: Option<(InternalKey, Bytes)> = None;
                    for v in value_rx {
                        if let Some((k, v)) = v? {
                            match result.as_mut() {
                                None => result = Some((k, v)),
                                Some((s, _)) => {
                                    if k.seq_num() >= s.seq_num() {
                                        result = Some((k, v));
                                    }
                                }
                            }
                        }
                    }
                    result
                } else {
                    for file in files.iter().rev() {
                        if let Ok(Some(entry)) = file.search(ukey, opts) {
                            return Ok(Some(entry));
                        }
                    }
                    None
                }
            }
            LevelMode::Leveled(files) => {
                // FIXME: use binary search
                let r = files
                    .iter()
                    .find(|x| x.may_contains(ikey.ukey, opts.get_comparator()));
                if let Some(f) = r
                {
                    if let Ok(Some(entry)) = f.search(ukey, opts) {
                        return Ok(Some(entry));
                    }
                }
                None
            }
        };
        Ok(r)
    }

    pub(crate) fn total_size(&self) -> u64 {
        self.as_ref().iter().map(|x| x.size).sum()
    }

    pub(crate) fn nums(&self) -> usize {
        self.as_ref().len()
    }

    pub(crate) fn add(&mut self, file: DataFilePtr<S>) {
        self.as_mut().push(file);
    }

    pub(crate) fn ensure_in_order(&mut self, c: InternalKeyComparator) {
        match self {
            LevelMode::Tiered(files) => {
                files.sort_by(|a: &DataFilePtr<S>, b: &DataFilePtr<S>| a.num.cmp(&b.num))
            }
            LevelMode::Leveled(files) => files.sort_by(|a: &DataFilePtr<S>, b: &DataFilePtr<S>| {
                c.compare(a.smallest.as_ref(), b.smallest.as_ref())
            }),
        }
    }
}

pub(crate) type VersionPtr<S> = Arc<Version<S>>;

#[derive(Debug)]
pub(crate) struct Version<S: Storage> {
    files: Vec<LevelMode<S>>,
    // pub(crate) compaction_score: Option<f64>,
    // pub(crate) compaction_level: Option<usize>,
    pub(crate) compaction: Option<(usize, f64)>,
    pub(crate) seek_compact: Option<(DataFilePtr<S>, usize)>,
    pub(crate) clean_trigger: Option<Sender<BGWorkTask>>,
}

impl<S: Storage> Default for Version<S> {
    fn default() -> Self {
        Self {
            files: (0..LEVELS)
                .map(|x| {
                    if x == 0 {
                        LevelMode::Tiered(vec![])
                    } else {
                        LevelMode::Leveled(vec![])
                    }
                })
                .collect(),
            compaction: None,
            seek_compact: None,
            clean_trigger: None,
        }
    }
}

// No clients need this version anymore, means some files in this version
// may have been compacted to new ones in the next level. So we can delete
// them now.
impl<S: Storage> Drop for Version<S> {
    fn drop(&mut self) {
        if let Some(tx) = self.clean_trigger.take() {
            let file_nums = self.all_file_nums().into_iter().collect::<Vec<u64>>();
            let _ = tx.send(BGWorkTask::TryCleanSSTs(file_nums));
        }
    }
}

impl<S: Storage> Version<S> {
    pub(crate) fn new(bgevent_trigger: Sender<BGWorkTask>) -> Self {
        let mut v = Version::default();
        v.clean_trigger = Some(bgevent_trigger);
        v
    }

    pub(crate) fn file_nums_in_level(&self, l: usize) -> usize {
        if l > LEVELS {
            panic!("level is too high and we have no files there");
        }
        self.files[l].nums()
    }

    pub(crate) fn all_file_nums(&self) -> HashSet<u64> {
        let mut res = HashSet::new();
        for files in self.files.iter() {
            files.as_ref().iter().for_each(|x| {
                let _ = res.insert(x.num);
            });
        }
        res
    }

    pub(crate) fn files_in_level(&self, l: usize) -> &Vec<DataFilePtr<S>> {
        self.files[l].as_ref()
    }

    pub(crate) fn edit(&self, ve: &VersionEdit<S>, icmp: InternalKeyComparator) -> Version<S> {
        let mut new_version = Version::default();
        for (level, files) in new_version.files.iter_mut().enumerate() {
            for file in self.files[level].as_ref() {
                if ve.deleted_files.contains(&DeleteFileEntry(level, file.num)) {
                    continue;
                }
                files.add(file.clone());
            }
        }
        for new_file in ve.new_files.iter() {
            new_version.files[new_file.0 as usize].add(new_file.1.clone());
        }

        for (_, level) in new_version.files.iter_mut().enumerate() {
            level.ensure_in_order(icmp);
        }
        new_version.update_compaction_score();
        new_version.clean_trigger = self.clean_trigger.clone();
        new_version
    }

    fn update_compaction_score(&mut self) {
        let l0_score =  (self.files[0].nums() as f64) / (L0_COMPACTION_TRIGGER as f64);
        // l0 level is not empty
        if l0_score >= 0.0 {
            self.compaction = Some((0, l0_score));
        }
        let mut max_bytes = (10 * 1024 * 1024) as f64;
        for (level, files) in self.files[1..].iter().enumerate() {
            let score = (files.total_size() as f64) / max_bytes;
            if let Some(s) = self.compaction.and_then(|(_, s)| Some(s)) {
                if score > s {
                    self.compaction = Some((level+1, score));
                }
            }
            max_bytes *= 10.0;
        }
    }

    pub(crate) fn overlaps(
        &self,
        level: usize,
        ucmp: ComparatorImpl,
        ukey0: &[u8],
        ukey1: &[u8],
    ) -> Vec<DataFilePtr<S>> {
        self.files[level]
            .as_ref()
            .iter()
            .filter(|x| {
                !((ucmp.compare(x.smallest.ukey().as_ref(), ukey1) == Ordering::Greater)
                    || (ucmp.compare(x.largest.ukey().as_ref(), ukey0) == Ordering::Less))
            })
            .map(|x| x.clone())
            .collect()
    }

    #[allow(unused)]
    fn check_ordering(&self, icmp: InternalKeyComparator) -> bool {
        for (level, files) in self.files.iter().enumerate() {
            let mut prev_file_num = 0u64;
            let mut prev_largest = InternalKey::default();
            for (i, file) in files.as_ref().iter().enumerate() {
                if icmp.compare(file.smallest.as_ref(), file.largest.as_ref()) == Ordering::Greater
                {
                    return false;
                }
                if i != 0 {
                    if prev_file_num >= file.num {
                        return false;
                    }
                    if level != 0
                        && icmp.compare(prev_largest.as_ref(), file.smallest.as_ref())
                            == Ordering::Greater
                    {
                        return false;
                    }
                }
                prev_file_num = file.num;
                prev_largest = file.largest.clone();
            }
        }
        true
    }
}

impl<S: Storage> Version<S> {
    // TODO need to collect statistics when querying the db files, in order to help to make decisions on compactions
    pub(crate) fn get(&self, ikey: InternalKeyRef, opts: &Opts) -> Result<Option<SSTEntry>, LError> {
        for files in self.files.iter() {
            if let Some(v) = files.search_in_level(&ikey, opts)? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }
}

// TODO use Option types to mask if a field exists instead of their zero value
#[derive(Default)]
pub(crate) struct VersionEdit<S: Storage> {
    pub comparator_name: String,
    // file number of WAL
    pub(crate) log_number: u64,
    pub(crate) next_file_number: u64,
    pub(crate) last_sequence: u64,
    pub(crate) compact_pointers: Vec<CompactPointerEntry>,
    pub(crate) deleted_files: HashSet<DeleteFileEntry>,
    pub(crate) new_files: Vec<NewFileEntry<S>>,
}

impl<S: Storage> Debug for VersionEdit<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ff = f.debug_struct("VersionEdit");
        if !self.comparator_name.is_empty() {
            ff.field("compactor_name", &self.comparator_name);
        }
        if self.log_number > 0 {
            ff.field("log_number", &self.log_number);
        }
        if self.next_file_number > 0 {
            ff.field("next_file_number", &self.next_file_number);
        }
        if self.last_sequence > 0 {
            ff.field("last_sequence", &self.last_sequence);
        }
        if !self.compact_pointers.is_empty() {
            ff.field("compact_pointers", &self.compact_pointers);
        }
        if !self.deleted_files.is_empty() {
            ff.field("deleted_files", &self.deleted_files);
        }
        if !self.new_files.is_empty() {
            let new_files = self
                .new_files
                .iter()
                .map(|x| (x.0, x.1.num))
                .collect::<Vec<(usize, u64)>>();
            ff.field("new_files", &new_files);
        }
        ff.finish()
    }
}

impl<S: Storage> VersionEdit<S> {
    pub(crate) fn empty() -> Self {
        Self {
            comparator_name: String::new(),
            log_number: 0,
            next_file_number: 0,
            last_sequence: 0,
            compact_pointers: vec![],
            deleted_files: HashSet::new(),
            new_files: vec![],
        }
    }

    pub(crate) fn set_log_number(&mut self, log_number: u64) {
        self.log_number = log_number;
    }

    pub(crate) fn new_file(&mut self, level: usize, f: DataFilePtr<S>) {
        self.new_files.push(NewFileEntry(level, f));
    }

    pub(crate) fn delete_file(&mut self, level: usize, num: u64) {
        self.deleted_files.insert(DeleteFileEntry(level, num));
    }

    pub(crate) fn set_next_file_number(&mut self, num: u64) {
        self.next_file_number = num;
    }

    pub(crate) fn set_comparator_name(&mut self, name: String) {
        self.comparator_name = name;
    }
}

impl<S: Storage> Encoding for VersionEdit<S> {
    fn encode(&self, dst: &mut BytesMut, _opts: &Opts) -> usize {
        let s = dst.len();
        if !self.comparator_name.is_empty() {
            put_uvarint(dst, TAG_COMPARATOR);
            put_uvarint(dst, self.comparator_name.len() as u64);
            dst.extend_from_slice(self.comparator_name.as_bytes());
        }
        if self.log_number != 0 {
            put_uvarint(dst, TAG_LOG_NUMBER);
            put_uvarint(dst, self.log_number);
        }

        if self.next_file_number != 0 {
            put_uvarint(dst, TAG_NEXT_FILE_NUMBER);
            put_uvarint(dst, self.next_file_number);
        }
        if self.last_sequence != 0 {
            put_uvarint(dst, TAG_LAST_SEQUENCE);
            put_uvarint(dst, self.last_sequence);
        }
        for cp in self.compact_pointers.iter() {
            put_uvarint(dst, TAG_COMPACT_POINTER);
            put_uvarint(dst, cp.0 as u64);
            put_uvarint(dst, cp.1.as_ref().len() as u64);
            dst.extend_from_slice(cp.1.as_ref());
        }
        for cp in self.deleted_files.iter() {
            put_uvarint(dst, TAG_DELETED_FILE);
            put_uvarint(dst, cp.0 as u64);
            put_uvarint(dst, cp.1);
        }
        for cp in self.new_files.iter() {
            put_uvarint(dst, TAG_NEW_FILE);
            put_uvarint(dst, cp.0 as u64);
            put_uvarint(dst, cp.1.num);
            put_uvarint(dst, cp.1.size as u64);
            put_uvarint(dst, cp.1.smallest.as_ref().len() as u64);
            dst.extend_from_slice(cp.1.smallest.as_ref());
            put_uvarint(dst, cp.1.largest.as_ref().len() as u64);
            dst.extend_from_slice(cp.1.largest.as_ref());
        }
        dst.len() - s
    }

    fn decode(src: &mut Bytes, _opts: &Opts) -> Result<Self, LError> {
        let mut ve = VersionEdit::empty();
        let must_take_uvarint = |src: &mut Bytes| -> Result<u64, LError> {
            take_uvarint(src).ok_or(LError::InvalidFile("the MANIFEST file is malformat".into()))
        };
        let e = LError::InvalidFile("the MANIFEST file is malformat".into());
        loop {
            match take_uvarint(src) {
                None => break,
                Some(tag) => match tag {
                    TAG_COMPARATOR => {
                        ve.comparator_name = {
                            let l = must_take_uvarint(src)?;
                            String::from_utf8(src.split_to(l as usize).to_vec())
                                .map_err(|_| e.clone())?
                        }
                    }
                    TAG_LOG_NUMBER => ve.log_number = must_take_uvarint(src)?,
                    TAG_NEXT_FILE_NUMBER => ve.next_file_number = must_take_uvarint(src)?,
                    TAG_LAST_SEQUENCE => ve.last_sequence = must_take_uvarint(src)?,
                    TAG_COMPACT_POINTER => {
                        let level = must_take_uvarint(src)? as usize;
                        let kl = must_take_uvarint(src)? as u8;
                        let key = src.split_to(kl as usize);
                        ve.compact_pointers
                            .push(CompactPointerEntry(level, InternalKey::try_from(key)?));
                    }
                    TAG_DELETED_FILE => {
                        ve.deleted_files.insert(DeleteFileEntry(
                            must_take_uvarint(src)? as usize,
                            must_take_uvarint(src)?,
                        ));
                    }
                    TAG_NEW_FILE => {
                        let level = must_take_uvarint(src)? as usize;
                        let file_num = must_take_uvarint(src)?;
                        let file_size = must_take_uvarint(src)?;
                        let sl = must_take_uvarint(src)?;
                        let smallest = src.split_to(sl as usize).try_into()?;
                        let ll = must_take_uvarint(src)?;
                        let largest = src.split_to(ll as usize).try_into()?;
                        ve.new_files.push(NewFileEntry(
                            level,
                            DataFilePtr::new(DataFile {
                                num: file_num,
                                size: file_size,
                                smallest: smallest,
                                largest: largest,
                                file: None,
                                key_stream: None,
                            }),
                        ));
                    }
                    _ => return Err(e),
                },
            }
        }
        Ok(ve)
    }
}

const TAG_COMPARATOR: u64 = 1;
const TAG_LOG_NUMBER: u64 = 2;
const TAG_NEXT_FILE_NUMBER: u64 = 3;
const TAG_LAST_SEQUENCE: u64 = 4;
const TAG_COMPACT_POINTER: u64 = 5;
const TAG_DELETED_FILE: u64 = 6;
const TAG_NEW_FILE: u64 = 7;

#[derive(Clone, Debug, Default)]
pub(crate) struct CompactPointerEntry(usize, InternalKey);
#[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) struct DeleteFileEntry(usize, u64);
#[derive(Clone, Debug)]
pub(crate) struct NewFileEntry<S: Storage>(usize, DataFilePtr<S>);

#[cfg(test)]
mod test {
    use crate::compare::BYTEWISE_COMPARATOR;
    use crate::io::MemFile;
    use crate::key::{InternalKeyComparator, InternalKeyRef};
    use crate::version::{DataFile, DataFilePtr, Version, VersionEdit};

    #[test]
    fn test_version_edit() {
        let icmp = InternalKeyComparator::from(BYTEWISE_COMPARATOR);
        let make_data_file = |num: u64| -> DataFilePtr<MemFile> {
            let sm = format!("{}", num * 10);
            let lg = format!("{}", (num + 1) * 10 - 1);
            DataFilePtr::new(DataFile {
                num,
                size: 0,
                smallest: InternalKeyRef::from((sm.as_bytes(), num * 10)).to_owned(),
                largest: InternalKeyRef::from((lg.as_bytes(), (num + 1) * 10 - 1)).to_owned(),
                file: None,
                key_stream: None,
            })
        };

        let version0: Version<MemFile> = Version::default();
        let mut ve0 = VersionEdit::empty();

        let files = (0..10)
            .map(|x| ((x % 4) as usize, make_data_file(x)))
            .collect::<Vec<(usize, DataFilePtr<MemFile>)>>();
        for (level, file) in files.iter() {
            ve0.new_file(*level, file.clone());
        }
        let version1 = version0.edit(&ve0, icmp);
        assert_eq!(
            version1
                .files_in_level(0)
                .iter()
                .map(|x| x.num)
                .collect::<Vec<u64>>(),
            vec![0u64, 4, 8]
        );
        assert_eq!(
            version1
                .files_in_level(1)
                .iter()
                .map(|x| x.num)
                .collect::<Vec<u64>>(),
            vec![1u64, 5, 9]
        );
        assert_eq!(
            version1
                .files_in_level(2)
                .iter()
                .map(|x| x.num)
                .collect::<Vec<u64>>(),
            vec![2u64, 6]
        );
        assert_eq!(
            version1
                .files_in_level(3)
                .iter()
                .map(|x| x.num)
                .collect::<Vec<u64>>(),
            vec![3u64, 7]
        );
        assert_eq!(version1.file_nums_in_level(4), 0);
        let mut ve1 = VersionEdit::empty();
        let new_files = (11..13)
            .map(|x| (2usize, make_data_file(x)))
            .collect::<Vec<(usize, DataFilePtr<MemFile>)>>();
        for (level, file) in new_files.iter() {
            ve1.new_file(*level, file.clone());
        }
        ve1.delete_file(0, 4);
        ve1.delete_file(1, 9);
        let version2 = version1.edit(&ve1, icmp);
        assert_eq!(
            version2
                .files_in_level(0)
                .iter()
                .map(|x| x.num)
                .collect::<Vec<u64>>(),
            vec![0u64, 8]
        );
        assert_eq!(
            version2
                .files_in_level(1)
                .iter()
                .map(|x| x.num)
                .collect::<Vec<u64>>(),
            vec![1u64, 5]
        );
        assert_eq!(
            version2
                .files_in_level(2)
                .iter()
                .map(|x| x.num)
                .collect::<Vec<u64>>(),
            vec![11u64, 12, 2, 6]
        );
        assert_eq!(
            version2
                .files_in_level(3)
                .iter()
                .map(|x| x.num)
                .collect::<Vec<u64>>(),
            vec![3u64, 7]
        );
        assert_eq!(version2.file_nums_in_level(4), 0);
    }
}
