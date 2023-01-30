use crate::compare::Comparator;
use crate::db::BGWorkTask;
use crate::filename::{db_filename, set_current_file, FileType};
use crate::io::{Encoding, Storage, StorageSystem};
use crate::key::{InternalKey, InternalKeyComparator, InternalKeyRef};
use crate::opts::Opts;
use crate::sstable::reader::{BlockCache, LRUCache, SSTableReader};
use crate::utils::varint::{put_uvarint, take_uvarint};
use crate::wal::{WalReader, WalWriter};
use crate::{LError, L0_COMPACTION_TRIGGER};
use bytes::{Bytes, BytesMut};
use crossbeam::channel::Sender;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

pub const LEVELS: usize = 7;

pub(crate) struct VersionSet<C: Comparator, O: Storage> {
    dirname: String,
    opts: Opts<C>,
    ucmp: C,
    #[allow(unused)]
    icmp: InternalKeyComparator<C>,
    version: VersionPtr<O>,
    clean_trigger: Sender<BGWorkTask>,

    pub(crate) block_cache: BlockCache,

    pub(crate) log_number: u64,
    pub(crate) next_file_number: u64,
    pub(crate) last_sequence: u64,

    // TODO: need to move this to Opts
    pub(crate) fs: &'static dyn StorageSystem<O = O>,

    pub(crate) manifest_file_number: u64,
    pub(crate) manifest_file: Arc<Mutex<Option<WalWriter<O>>>>,
}

impl<C: Comparator, O: Storage> VersionSet<C, O> {
    pub(crate) fn new(
        dirname: String,
        event_trigger: Sender<BGWorkTask>,
        opts: Opts<C>,
        fs: &'static dyn StorageSystem<O = O>,
    ) -> Self {
        Self {
            dirname: dirname,
            ucmp: opts.get_comparator(),
            icmp: InternalKeyComparator::from(opts.get_comparator()),
            block_cache: LRUCache::new(opts.get_block_cache_limit()),
            opts: opts,
            version: Arc::new(Version::default()),
            log_number: 0,
            next_file_number: 0,
            last_sequence: 0,
            manifest_file_number: 0,
            manifest_file: Arc::new(Mutex::new(None)),
            clean_trigger: event_trigger,
            fs,
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

    pub(crate) fn load(&mut self, opts: Opts<C>) -> Result<(), LError> {
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
            version = version.edit(&ve);
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
            for i in 0..files.len() {
                if files[i].file.is_none() || files[i].clean_trigger.is_none() {
                    let table_name =
                        db_filename(self.dirname.as_str(), FileType::Table(files[i].num));
                    let table = self.fs.open(table_name.as_str())?;
                    let mut reader = SSTableReader::open(table, files[i].num, &self.opts)?;
                    reader.set_cache_handle(self.block_cache.clone());
                    let opened_file = DataFilePtr::new(DataFile {
                        num: files[i].num,
                        size: files[i].size,
                        smallest: files[i].smallest.clone(),
                        largest: files[i].largest.clone(),
                        file: Some(reader),
                        clean_trigger: Some(self.clean_trigger.clone()),
                    });
                    files[i] = opened_file;
                }
            }
        }
        self.set_version(Arc::new(version));
        Ok(())
    }

    pub(crate) fn make_snapshot(&self) -> VersionEdit<O> {
        let mut ve = VersionEdit::empty();
        for (level, files) in self.current_version().files.iter().enumerate() {
            files.iter().for_each(|x| ve.new_file(level, x.clone()));
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

#[derive(Clone)]
pub(crate) struct DataFile<S: Storage> {
    pub(crate) num: u64,
    pub(crate) size: u64,
    pub(crate) smallest: InternalKey,
    pub(crate) largest: InternalKey,
    pub(crate) file: Option<SSTableReader<S>>,
    pub(crate) clean_trigger: Option<Sender<BGWorkTask>>,
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

// nobody need this file, meaning that we can delete it now.
// FIXME we need to avoid deleting a sst file when the process exits.
impl<T: Storage> Drop for DataFile<T> {
    fn drop(&mut self) {
        if self.file.is_some() {
            if let Some(tx) = self.clean_trigger.take() {
                let _ = tx.send(BGWorkTask::CleanSST(self.num));
            }
        }
    }
}

pub(crate) fn ikey_range<C: Comparator, S: Storage>(
    icmp: InternalKeyComparator<C>,
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

pub(crate) type VersionPtr<S> = Arc<Version<S>>;

#[derive(Debug)]
pub(crate) struct Version<S: Storage> {
    files: Vec<Vec<DataFilePtr<S>>>,
    pub(crate) compaction_score: f64,
    pub(crate) compaction_level: usize,
    pub(crate) seek_compact: Option<(DataFilePtr<S>, usize)>,
}

impl<S: Storage> Default for Version<S> {
    fn default() -> Self {
        Self {
            files: (0..LEVELS).map(|_x| vec![]).collect(),
            compaction_level: 0,
            compaction_score: 0f64,
            seek_compact: None,
        }
    }
}

impl<S: Storage> Version<S> {
    pub(crate) fn file_nums_in_level(&self, l: usize) -> usize {
        if l > LEVELS {
            panic!("level is too high and we have no files there");
        }
        self.files[l].len()
    }

    pub(crate) fn files_in_level(&self, l: usize) -> &Vec<DataFilePtr<S>> {
        self.files[l].as_ref()
    }

    pub(crate) fn edit(&self, ve: &VersionEdit<S>) -> Version<S> {
        let mut new_version = Version::default();
        for (level, files) in new_version.files.iter_mut().enumerate() {
            for file in self.files[level].iter() {
                if ve.deleted_files.contains(&DeleteFileEntry(level, file.num)) {
                    continue;
                }
                files.push(file.clone());
            }
        }
        for new_file in ve.new_files.iter() {
            new_version.files[new_file.0 as usize].push(new_file.1.clone());
        }

        for (_, files) in new_version.files.iter_mut().enumerate() {
            files.sort_by(|a, b| a.num.cmp(&b.num));
        }
        new_version.update_compaction_score();
        new_version
    }

    fn update_compaction_score(&mut self) {
        self.compaction_score = (self.files[0].len() as f64) / (L0_COMPACTION_TRIGGER as f64);
        self.compaction_level = 0;
        let mut max_bytes = (10 * 1024 * 1024) as f64;
        for level in 1..self.files.len() {
            let files = &self.files[level];
            let total_size = files.iter().map(|x| x.size).sum::<u64>();
            let score = (total_size as f64) / max_bytes;
            if score > self.compaction_score {
                self.compaction_score = score;
                self.compaction_level = level;
            }
            max_bytes *= 10.0;
        }

        for (level, files) in self.files[1..].iter().enumerate() {
            let total_size = files.iter().map(|x| x.size).sum::<u64>();
            let score = (total_size as f64) / max_bytes;
            if score > self.compaction_score {
                self.compaction_score = score;
                self.compaction_level = level;
            }
            max_bytes *= 10.0;
        }
    }

    pub(crate) fn overlaps<C: Comparator>(
        &self,
        level: usize,
        ucmp: C,
        ukey0: &[u8],
        ukey1: &[u8],
    ) -> Vec<DataFilePtr<S>> {
        self.files[level]
            .iter()
            .filter(|x| {
                !((ucmp.compare(x.smallest.ukey().as_ref(), ukey1) == Ordering::Greater)
                    || (ucmp.compare(x.largest.ukey().as_ref(), ukey0) == Ordering::Less))
            })
            .map(|x| x.clone())
            .collect()
    }

    #[allow(unused)]
    fn check_ordering<C: Comparator>(&self, icmp: InternalKeyComparator<C>) -> bool {
        for (level, files) in self.files.iter().enumerate() {
            let mut prev_file_num = 0u64;
            let mut prev_largest = InternalKey::default();
            for (i, file) in files.iter().enumerate() {
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
    pub(crate) fn get<C: Comparator>(
        &self,
        ikey: InternalKeyRef,
        opts: &Opts<C>,
    ) -> Result<Option<Bytes>, LError> {
        let ukey = ikey.ukey;
        let ucmp = opts.get_comparator();
        let ik = ikey.to_owned();
        let may_find = |file: &DataFilePtr<S>| -> bool {
            let r1 = ucmp.compare(ukey, file.smallest.ukey()) == Ordering::Less;
            let r2 = ucmp.compare(ukey, file.largest.ukey()) == Ordering::Greater;
            !(r1 || r2)
        };
        let search_in_file = |file: &DataFilePtr<S>| -> Result<Option<Bytes>, LError> {
            match file.file.as_ref() {
                None => Err(LError::Internal("db file is not opened".to_string())),
                Some(f) => f.get(ik.as_ref(), opts),
            }
        };
        for file in self.files[0].iter().rev() {
            if may_find(file) {
                if let Ok(Some(v)) = search_in_file(file) {
                    return Ok(Some(v));
                }
            }
        }
        // search the left levels
        for files in self.files[1..].iter() {
            // FIXME maybe binary search is better?
            if let Some(f) = files.iter().find(|x| may_find(x)) {
                if let Ok(Some(v)) = search_in_file(f) {
                    return Ok(Some(v));
                }
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
    fn encode<T: Comparator>(&self, dst: &mut BytesMut, _opts: &Opts<T>) -> usize {
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

    fn decode<T: Comparator>(src: &mut Bytes, _opts: &Opts<T>) -> Result<Self, LError> {
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
                                clean_trigger: None,
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
    use crate::io::MemFile;
    use crate::key::InternalKeyRef;
    use crate::version::{DataFile, DataFilePtr, Version, VersionEdit};

    #[test]
    fn test_version_edit() {
        let make_data_file = |num: u64| -> DataFilePtr<MemFile> {
            let sm = format!("{}", num * 10);
            let lg = format!("{}", (num + 1) * 10 - 1);
            DataFilePtr::new(DataFile {
                num,
                size: 0,
                smallest: InternalKeyRef::from((sm.as_bytes(), num * 10)).to_owned(),
                largest: InternalKeyRef::from((lg.as_bytes(), (num + 1) * 10 - 1)).to_owned(),
                file: None,
                clean_trigger: None,
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
        let version1 = version0.edit(&ve0);
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
        let version2 = version1.edit(&ve1);
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
            vec![2u64, 6, 11, 12]
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
