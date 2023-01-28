use crate::io::{Storage, StorageSystem};
use crate::LError;

pub enum FileType {
    WAL(u64),
    Lock,
    Table(u64),
    Current,
    Manifest(u64),
}

pub(crate) fn db_filename(dirname: &str, file_type: FileType) -> String {
    let mut dirname = dirname.to_string();
    if let Some(b'/') = dirname.as_bytes().last() {
        dirname.pop();
    }
    let file_name = match file_type {
        FileType::Table(num) => format!("{n:>0w$}.ldb", n = num, w = 6),
        FileType::WAL(num) => format!("{n:>0w$}.log", n = num, w = 6),
        FileType::Manifest(num) => format!("MANIFEST-{n:>0w$}", n = num, w = 6),
        FileType::Current => "CURRENT".to_string(),
        FileType::Lock => "LOCK".to_string(),
    };
    format!("{}/{}", dirname, file_name)
}

pub(crate) fn parse_dbname(fullname: &str) -> Option<FileType> {
    let filename = if fullname.contains("/") {
        fullname
            .split_terminator("/")
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
            .pop()
            .unwrap()
    } else {
        fullname.to_string()
    };
    match filename.as_str() {
        "LOCK" => Some(FileType::Lock),
        "CURRENT" => Some(FileType::Current),
        other => {
            if other.contains(".") {
                let segs = other
                    .split(".")
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>();
                if segs.len() != 2 {
                    return None;
                }
                let num = match segs[0].parse::<u64>() {
                    Ok(n) => n,
                    Err(_) => return None,
                };
                match segs[1].as_str() {
                    "ldb" => Some(FileType::Table(num)),
                    "log" => Some(FileType::WAL(num)),
                    _ => None,
                }
            } else if other.starts_with("MANIFEST-") {
                let a = other.trim_start_matches("MANIFEST-");
                a.parse::<u64>().ok().map(|x| FileType::Manifest(x))
            } else {
                None
            }
        }
    }
}

pub(crate) fn set_current_file<O: Storage, S: StorageSystem<O = O>>(
    fs: &S,
    dirname: &str,
    num: u64,
) -> Result<(), LError> {
    let new_name = db_filename(dirname, FileType::Current);
    let old_name = format!("{}.{n:>0w$}.dbtmp", new_name, n = num, w = 6);
    let _ = fs.remove(old_name.as_str());
    let mut f = fs.create(old_name.as_str())?;
    let c = format!("MANIFEST-{n:>0w$}\n", n = num, w = 6);
    f.write_all(c.as_bytes())?;
    fs.rename(old_name.as_str(), new_name.as_str())?;
    Ok(())
}
