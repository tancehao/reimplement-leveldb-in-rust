pub mod format;
pub mod reader;
pub mod writer;

#[cfg(test)]
mod test {
    use crate::compare::{BytewiseComparator, Comparator};
    use crate::db::DBScanner;
    use crate::io::{MemFS, StorageSystem};
    use crate::key::{InternalKey, InternalKeyComparator, InternalKeyRef};
    use crate::opts::{Opts, OptsRaw};
    use crate::sstable::reader::{SSTableReader, SSTableScanner};
    use crate::sstable::writer::SSTableWriter;
    use crate::LError;

    use bytes::Bytes;
    use std::collections::HashMap;

    #[test]
    fn test_sstable() {
        assert!(write_and_read().is_ok());
    }

    fn write_and_read() -> Result<(), LError> {
        let mut opt_raw: OptsRaw<BytewiseComparator> = OptsRaw::default();
        opt_raw.block_size = 10240;
        let opt = Opts::new(opt_raw);
        let fs = MemFS::default();
        let file = fs.create("sstable")?;

        let mut writer = SSTableWriter::new(1, file, opt.clone());
        let mut kvs_list: Vec<(InternalKey, Bytes)> = (0..10000)
            .map(|x| {
                let (uk, value) = (format!("key:{}", x), format!("value:{}{}{}", x, x, x));
                let ik = InternalKeyRef::from((uk.as_bytes(), x as u64)).to_owned();
                (ik, value.into())
            })
            .collect();
        let icmp = InternalKeyComparator::from(opt.get_comparator());
        kvs_list.sort_by(|(ka, _), (kb, _)| icmp.compare(ka.as_ref(), kb.as_ref()));
        let mut kvs_map = HashMap::new();
        for (k, v) in kvs_list.iter() {
            kvs_map.insert(k.clone(), v.clone());
            writer.set(k, v.clone())?;
        }
        writer.finish().unwrap();
        let file = fs.open("sstable")?;
        let reader = SSTableReader::open(file, 1, &opt)?;
        for (k, v) in kvs_map.iter() {
            assert_eq!(reader.get(k.as_ref(), &opt)?, Some(v.clone()));
        }
        let mut scanner = SSTableScanner::new(reader, opt.clone());
        for (k, v) in kvs_list.iter() {
            assert_eq!(scanner.next()?, Some((k.clone(), v.clone())));
        }
        Ok(())
    }
}
