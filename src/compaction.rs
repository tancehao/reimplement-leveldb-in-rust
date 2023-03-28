use crate::compare::ComparatorImpl;
use crate::db::{BGWorkTask, ConcatScanner, DBScanner, MergeScanner, DB};
use crate::filename::{db_filename, FileType};
use crate::io::Storage;
use crate::key::{InternalKey, InternalKeyComparator, InternalKeyKind, InternalKeyRef};
use crate::memtable::MemTable;
use crate::sstable::reader::SSTableScanner;
use crate::sstable::writer::SSTableWriter;
use crate::utils::call_on_drop::CallOnDrop;
use crate::version::{ikey_range, DataFile, DataFilePtr, VersionEdit, VersionPtr, LEVELS};
use crate::{call_on_drop, unregister, LError};
use std::cmp::Ordering;
use std::collections::btree_set::BTreeSet;

use bytes::Bytes;
use crossbeam::channel::unbounded;
use std::fmt::{Debug, Formatter};
use std::ops::Bound::{Excluded, Included};

const TARGET_FILE_SIZE: u64 = 2 * 1024 * 1024;
const MAX_GRANDPARENT_OVERLAP_BYTES: u64 = 10 * TARGET_FILE_SIZE;
const EXPAND_COMPACTION_BYTE_SIZE_LIMIT: u64 = 25 * TARGET_FILE_SIZE;

pub struct Compaction<S: Storage> {
    version: VersionPtr<S>,
    level: usize,
    #[allow(unused)]
    snapshot_points: Vec<u64>,
    inputs: Vec<Vec<DataFilePtr<S>>>, // at most 3 elements
    snapshots: BTreeSet<u64>,
}

impl<S: Storage> Debug for Compaction<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Compaction")
            .field("level", &self.level)
            .field("inputs", &self.inputs)
            .finish()
    }
}

impl<S: Storage> Compaction<S> {
    pub(crate) fn pick_from_version(
        version: VersionPtr<S>,
        ucmp: ComparatorImpl,
        icmp: InternalKeyComparator,
    ) -> Option<Self> {
        let mut c = Compaction {
            version: version.clone(),
            snapshot_points: vec![],
            level: 0,
            inputs: vec![vec![], vec![], vec![]],
            snapshots: BTreeSet::new(),
        };
        if let Some((level, _)) = version
            .compaction
            .and_then(|(l, s)| (s >= 1.0).then_some((l, s)))
        {
            // we pick this level because there are too much data in this level.
            // and we just start from the first file in this group.
            c.level = level;
            c.inputs[0] = vec![version.files_in_level(level)[0].clone()];
        } else if version.seek_compact.is_some() {
            let (file, level) = version.seek_compact.as_ref().unwrap();
            c.level = *level;
            c.inputs[0].push(file.clone());
        } else {
            return None;
        }
        if c.level == 0 {
            let (smallest, largest) = ikey_range(icmp, &c.inputs[0], None);
            c.inputs[0] = version.overlaps(0, ucmp, smallest.ukey(), largest.ukey());
        }
        c.setup_other_inputs(ucmp, icmp);
        Some(c)
    }

    fn setup_other_inputs(&mut self, ucmp: ComparatorImpl, icmp: InternalKeyComparator) {
        let (sm0, lg0) = ikey_range(icmp, &self.inputs[0], None);
        self.inputs[1] = self
            .version
            .overlaps(self.level + 1, ucmp, sm0.ukey(), lg0.ukey());
        let (mut sm1, mut lg1) = ikey_range(icmp, &self.inputs[0], Some(&self.inputs[1]));
        // Grow the inputs if it doesn't affect the number of level+1 files.
        if self.grow(ucmp, icmp, sm1.ukey(), lg1.ukey()) {
            (sm1, lg1) = ikey_range(icmp, &self.inputs[0], Some(&self.inputs[1]));
        }
        if self.level + 2 < LEVELS {
            self.inputs[2] = self
                .version
                .overlaps(self.level + 2, ucmp, sm1.ukey(), lg1.ukey());
        }
        // TODO update the compaction pointer for self.level
    }

    fn grow(
        &mut self,
        ucmp: ComparatorImpl,
        icmp: InternalKeyComparator,
        uk_small: &[u8],
        uk_large: &[u8],
    ) -> bool {
        if self.inputs[1].is_empty() {
            return false;
        }
        let g0 = self.version.overlaps(self.level, ucmp, uk_small, uk_large);
        if g0.len() <= self.inputs[0].len() {
            return false;
        }
        let g0_size = g0.iter().map(|x| x.size).sum::<u64>();
        let i1_size = self.inputs[1].iter().map(|x| x.size).sum::<u64>();
        if g0_size + i1_size >= EXPAND_COMPACTION_BYTE_SIZE_LIMIT {
            return false;
        }
        let (sm, lg) = ikey_range(icmp, g0.as_ref(), None);
        let g1 = self
            .version
            .overlaps(self.level + 1, ucmp, sm.ukey(), lg.ukey());
        if g1.len() != self.inputs[1].len() {
            return false;
        }
        self.inputs[0] = g0;
        self.inputs[1] = g1;
        true
    }

    fn is_base_level_for_ukey(&self, icmp: InternalKeyComparator, ikey: &InternalKey) -> bool {
        for level in (self.level + 2)..LEVELS {
            for file in self.version.files_in_level(level) {
                if file.may_contains(ikey, icmp) {
                    return false;
                }
            }
        }
        true
    }
}

impl<O: Storage, M: MemTable> DB<O, M> {
    pub(crate) fn maybe_schedule_compaction(&self) {
        if self.bgwork_trigger.send(BGWorkTask::Compaction).is_err() {
            panic!("bg work thread is broken")
        }
    }

    pub(crate) fn wait_imem_compaction_finish(&self) {
        let compactioning = self.compacting_imem.clone();
        let mut pending = compactioning.0.lock().unwrap();
        while *pending {
            pending = compactioning.1.wait(pending).unwrap();
        }
    }

    pub(crate) fn compact(&self) -> Result<(), LError> {
        let is_mem = self.compact1()?;
        if is_mem {
            self.compact1()?;
        }
        Ok(())
    }

    fn compact1(&self) -> Result<bool, LError> {
        let inner = self.inner.lock()?;
        if let Some(imem) = inner.imem.as_ref() {
            let imem = imem.clone();
            // when a memdb is full, we convert it to imem and give it a new empty one with a newly created WAL.
            // and cache the log number of newly create WAL to `versions.log_number`.
            // this log number will be persisted to manifest file AFTER the compaction has finished.
            // when replaying WALs, we skip those files with a log number less than this one.
            let current_log_number = inner.versions.log_number;
            // the log number of the WAL who's associated memtable is going to be compacted.
            let compacted_log_num = imem.wal_log_num();
            // no need to hold the lock on inner when compacting imem table
            drop(inner);
            self.compact_imem_table(imem.as_ref(), current_log_number)?;
            // now we have succeed in compacting the imem table to a file.
            // and it's associated WAL can be safely deleted.
            if self
                .bgwork_trigger
                .send(BGWorkTask::CleanWAL(compacted_log_num))
                .is_err()
            {
                panic!("bg work thread is broken")
            }

            self.inner.lock()?.imem.take();
            return Ok(true);
        }
        // TODO: support manual compactions
        let mut compaction = match Compaction::pick_from_version(
            inner.versions.current_version_cloned(),
            self.ucmp,
            self.icmp,
        ) {
            Some(c) => c,
            None => return Ok(false),
        };
        compaction.snapshots = inner.snapshots.0.clone();
        let total_size = compaction.inputs[2].iter().map(|x| x.size).sum::<u64>();
        // We can simply move the sstable to the next level, since there are no sstables in that level.
        // But we don't do this if the total size of the file in the 'grandparent' level is too large,
        // because this may lead to a very heavy compaction later. Instead we do compaction in the base
        // file hoping to decrease the size of the generated file as most as we can by remove those
        // deleted keys.
        if compaction.inputs[0].len() == 1
            && compaction.inputs[1].len() == 0
            && total_size < MAX_GRANDPARENT_OVERLAP_BYTES
        {
            let mut ve = VersionEdit::empty();
            ve.delete_file(compaction.level, compaction.inputs[0][0].num);
            ve.new_file(compaction.level + 1, compaction.inputs[0][0].clone());
            drop(inner);
            self.apply_version_edit(&mut ve)?;
            return Ok(false);
        }
        drop(inner);
        self.compact_disk_tables(compaction)?;
        Ok(false)
    }

    fn new_sstable_file(&self) -> Result<(u64, SSTableWriter<O>, CallOnDrop), LError> {
        let (file_num, file_name, fs) = {
            let mut inner = self.inner.lock()?;
            let file_num = inner.versions.incr_file_number();
            let file_name = db_filename(self.dirname.as_str(), FileType::Table(file_num));
            inner.pending_outputs.insert(file_num);
            (file_num, file_name, inner.versions.fs.clone())
        };
        let file = fs.create(file_name.as_str())?;
        let tmpfile_recycler = call_on_drop!({
            let _ = fs.remove(file_name.as_str());
        });
        let writer = SSTableWriter::new(file_num, file, self.opts.clone());
        Ok((file_num, writer, tmpfile_recycler))
    }

    pub(crate) fn compact_imem_table(&self, imem: &M, log_num: u64) -> Result<(), LError> {
        let compactioning = self.compacting_imem.clone();
        *compactioning.0.lock().unwrap() = true;
        call_on_drop!({
            *compactioning.0.lock().unwrap() = false;
            compactioning.1.notify_all();
        });

        let (file_num, mut new_table, mut tmpfile_recycler) = self.new_sstable_file()?;
        let mut scanner = imem.iter(self.ucmp);
        let mut smallest: Option<InternalKey> = None;
        let mut largest: Option<InternalKey> = None;

        while let Some((ukey, seq_num, value)) = scanner.next() {
            let (k, v) = match value {
                None => (InternalKeyKind::Delete, Bytes::new()),
                Some(v) => (InternalKeyKind::Set, v),
            };
            let internal_key = InternalKeyRef {
                ukey: ukey.as_ref(),
                k,
                seq_num,
            }
            .to_owned();
            smallest = smallest.or(Some(internal_key.clone()));
            largest = Some(internal_key.clone());
            new_table.set(&internal_key, v)?;
        }
        if smallest.is_none() || largest.is_none() {
            return Err(LError::Internal("compacting an empty imem".into()));
        }

        new_table.finish()?;
        let mut ve = VersionEdit::empty();
        let size = new_table.wrote_size() as u64;
        let mut reader = new_table.freeze()?;
        reader.set_cache_handle(self.inner.lock()?.versions.block_cache.clone());
        let (tx, rx) = unbounded();
        let df = DataFile {
            num: file_num,
            size,
            smallest: smallest.unwrap(),
            largest: largest.unwrap(),
            file: Some(reader),
            key_stream: Some(tx),
        };
        self.metric.incr_opened_files();
        let dfp = DataFilePtr::new(df);
        let file = dfp.file.clone();
        let metric = self.metric.clone();
        std::thread::spawn(move || {
            metric.incr_threads_cnt();
            DataFile::serve_queries(file, rx);
            metric.decr_threads_cnt();
        });
        ve.new_file(0, dfp);
        ve.set_log_number(log_num);
        self.apply_version_edit(&mut ve)?;
        unregister!(tmpfile_recycler);
        self.inner.lock()?.pending_outputs.remove(&file_num);
        Ok(())
    }

    pub(crate) fn compact_disk_tables(&self, c: Compaction<O>) -> Result<(), LError> {
        let opts = self.opts.clone();
        let files2scanner = |x: &[DataFilePtr<O>]| -> Vec<Box<dyn DBScanner>> {
            x.iter()
                .map(|x| x.file.as_ref().unwrap().clone())
                .map(|x| Box::new(SSTableScanner::new(x, opts.clone())) as Box<dyn DBScanner>)
                .collect()
        };

        let scanner0: Box<dyn DBScanner> = if c.level != 0 {
            Box::new(ConcatScanner::from(files2scanner(&c.inputs[0])))
        } else {
            Box::new(MergeScanner::new(
                self.icmp.clone(),
                files2scanner(&c.inputs[0]),
            ))
        };
        let scanner1: Box<dyn DBScanner> =
            Box::new(ConcatScanner::from(files2scanner(&c.inputs[1])));
        let mut scanner = Box::new(MergeScanner::new(
            self.icmp.clone(),
            vec![scanner0, scanner1],
        ));

        let mut smallest = None;
        let mut prev_key: Option<InternalKey> = None;
        let mut new_table: Option<(u64, SSTableWriter<O>)> = None;
        let mut new_files = vec![];
        let mut tmpfile_recyclers = vec![];
        while let Some((ik, v)) = scanner.next()? {
            if let Some(prev) = prev_key.as_ref() {
                if self.ucmp.compare(prev.ukey(), ik.ukey()) == Ordering::Equal {
                    // check if an older version of the same user-key should be kept
                    let captured = c
                        .snapshots
                        .range((Included(ik.seq_num()), Excluded(prev.seq_num())))
                        .next()
                        .is_some();
                    if !captured {
                        // the older version is not captured by some snapshot. ignore it.
                        continue;
                    }
                }
            }
            smallest = smallest.or(Some(ik.clone()));
            prev_key = Some(ik.clone());
            // no need to keep the deleted key because there are no older versions of SET operation.
            if ik.kind() == InternalKeyKind::Delete && c.is_base_level_for_ukey(self.icmp, &ik) {
                continue;
            }
            if new_table.is_none() {
                let (file_num, table, tmpfile_recycler) = self.new_sstable_file()?;
                tmpfile_recyclers.push(tmpfile_recycler);
                new_table = Some((file_num, table));
            }
            let t = new_table.as_mut().map(|(_, x)| x).unwrap();
            t.set(&ik, v)?;
            if t.wrote_size() >= opts.get_max_file_size() {
                t.finish()?;
                let (file_num, table) = new_table.take().unwrap();
                new_files.push((
                    file_num,
                    table,
                    smallest.take().unwrap(),
                    prev_key.take().unwrap(),
                ));
            }
        }
        if let Some((file_num, mut table)) = new_table {
            table.finish()?;
            new_files.push((
                file_num,
                table,
                smallest.take().unwrap(),
                prev_key.take().unwrap(),
            ));
        }
        let mut ve = VersionEdit::empty();
        let mut new_file_nums = vec![];
        for (file_num, new_table, smallest, largest) in new_files.into_iter() {
            new_file_nums.push(file_num);
            let size = new_table.wrote_size();
            let mut reader = new_table.freeze()?;
            reader.set_cache_handle(self.inner.lock()?.versions.block_cache.clone());
            ve.new_file(
                c.level + 1,
                DataFilePtr::new(DataFile {
                    num: file_num,
                    size,
                    smallest,
                    largest,
                    file: Some(reader),
                    key_stream: None,
                }),
            );
        }
        for (i, files) in c.inputs[..2].iter().enumerate() {
            files
                .iter()
                .for_each(|x| ve.delete_file(i + c.level, x.num));
        }
        self.apply_version_edit(&mut ve)?;

        let mut inner = self.inner.lock()?;
        new_file_nums.iter().for_each(|x| {
            inner.pending_outputs.remove(x);
        });

        tmpfile_recyclers.iter_mut().for_each(|x| unregister!(x));
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::db::{DBScanner, Snapshot, DB};
    use crate::io::{MemFS, MemFile, Storage, StorageSystem, MEM_FS};
    use crate::key::InternalKeyRef;
    use crate::memtable::simple::BTMap;
    use crate::memtable::MemTable;
    use crate::opts::{OptsRaw, ReadOptions, WriteOptions};
    use crate::sstable::reader::{SSTableReader, SSTableScanner};
    use bytes::Bytes;
    use lazy_static::lazy_static;
    use std::collections::HashMap;

    use std::sync::Arc;

    #[test]
    fn test_compact_imem() {
        let opt_raw: OptsRaw = OptsRaw::default();
        let opts = Arc::new(opt_raw);
        lazy_static! {
            pub(crate) static ref TEST_MEM_FS: MemFS = MemFS::default();
        }
        let fs = &*TEST_MEM_FS;
        let db = DB::open("db_dir".to_string(), opts.clone(), fs).unwrap();
        let mut mem = BTMap::default();
        let mut kvs = HashMap::new();
        for i in 1..10000 {
            let uk = format!("key:{}", i);
            let value = format!("value:{}:{}:{}", i, i, i);
            let ik = InternalKeyRef::from((uk.as_ref(), i)).to_owned();
            kvs.insert(ik, value.clone());
            mem.set(uk.into(), i, Some(value.into()));
        }
        let r = db.compact_imem_table(&mem, 1);
        assert!(r.is_ok());
        let file_list = fs.list("db_dir").unwrap();
        let fno = file_list
            .iter()
            .filter(|x| x.ends_with(".ldb"))
            .take(1)
            .next();
        assert!(fno.is_some());
        let file_name = fno.unwrap().clone();
        let mut f = fs
            .open(format!("./db_dir/{}", file_name.as_str()).as_str())
            .unwrap();
        f.seek(0).unwrap();
        assert!(f.size().unwrap() > 0);
        let r = SSTableReader::open(f, 2, &opts).unwrap();
        let mut s = SSTableScanner::new(r, opts.clone());
        while let Some((k, v)) = s.next().unwrap() {
            assert_eq!(kvs.get(&k).map(|x| x.as_bytes()), Some(v.as_ref()));
        }
    }

    #[test]
    fn test_compact_disk_tables() {
        let wo = WriteOptions::default();
        let mut opt_raw: OptsRaw = OptsRaw::default();
        opt_raw.write_buffer_size = 128;
        opt_raw.block_size = 512;
        opt_raw.max_file_size = 2048;
        let opts = Arc::new(opt_raw);
        let fs = &*MEM_FS as &'static dyn StorageSystem<O = MemFile>;
        let db: DB<MemFile, BTMap> = DB::open("disk_tables".to_string(), opts.clone(), fs).unwrap();
        let mut kvs = HashMap::new();
        for i in 1..10000 {
            let uk = format!("key:{}", i);
            let value = format!("value:{}:{}:{}", i, i, i);
            let ik = InternalKeyRef::from((uk.as_ref(), i)).to_owned();
            kvs.insert(ik, value.clone());
            assert!(db.set(&wo, uk.into(), value.into()).is_ok());
        }
        let r = db.compact1();
        assert!(r.is_ok());
        for (i, v) in kvs.iter() {
            let vv = Bytes::from(v.clone());
            if let Ok(Some(vvv)) = db.get(&ReadOptions::default(), i.ukey()) {
                assert_eq!(vvv.as_ref(), vv.as_ref());
            } else {
                panic!("not equal");
            }
        }
    }

    #[test]
    fn test_compact_wont_delete_keys_in_snapshot() {
        let wo = WriteOptions::default();
        let mut opt_raw: OptsRaw = OptsRaw::default();
        opt_raw.write_buffer_size = 128;
        opt_raw.block_size = 512;
        opt_raw.max_file_size = 2048;
        let opts = Arc::new(opt_raw);
        let fs = &*MEM_FS as &'static dyn StorageSystem<O = MemFile>;
        let db: DB<MemFile, BTMap> = DB::open("disk_tables".to_string(), opts.clone(), fs).unwrap();
        let mut kvs = HashMap::new();
        let mut snapshot: Option<Snapshot> = None;
        for i in 1..10000 {
            let uk = "key";
            let value = format!("{}", i);
            let ik = InternalKeyRef::from((uk.as_ref(), i)).to_owned();
            kvs.insert(ik, value.clone());
            assert!(db.set(&wo, uk.into(), value.into()).is_ok());
            if i == 10 {
                snapshot = Some(db.snapshot());
            }
        }
        assert!(db.compact1().is_ok());
        let mut ro = ReadOptions::default();
        ro.set_snapshot(snapshot.unwrap());
        let r = db.get(&ro, "key".as_ref());
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), Some("10".into()));
    }
}
