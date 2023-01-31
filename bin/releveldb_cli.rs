use bytes::Bytes;
use crossbeam::channel::Sender;
use releveldb::compare::BytewiseComparator;
use releveldb::db::{DBScanner, DB};
use releveldb::io::{OsFS, StorageSystem, OS_FS};
use releveldb::memtable::simple::BTMap;
use releveldb::opts::{empty_compact_hook, Opts, OptsRaw};
use releveldb::sstable::reader::SSTableReader;
use releveldb::sstable::reader::SSTableScanner;
use releveldb::utils::any::Any;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Arc;

fn interactive() {
    let opts = get_opts(Any::new(()));
    let dirname = format!("{}/data", OsFS::default().pwd().unwrap());
    let db: DB<BytewiseComparator, File, BTMap> =
        DB::open(dirname.clone(), opts.clone(), &OS_FS).unwrap();
    let mut cmd = String::new();
    loop {
        cmd.clear();
        write!(std::io::stdout(), "cmd>").unwrap();
        std::io::stdout().flush().unwrap();
        std::io::stdin().read_line(&mut cmd).unwrap();
        cmd = cmd.trim_matches(|x| x == '\r' || x == '\n').to_string();
        let params = cmd
            .split(" ")
            .filter(|x| !x.is_empty())
            .map(|x| x.to_string())
            .collect::<Vec<String>>();

        let mut params = params.into_iter();
        let cmd_name = match params.next().map(|x| x.to_lowercase()) {
            None => continue,
            Some(v) => v,
        };
        match cmd_name.as_str() {
            "get" => match params.next() {
                None => println!("key should be specified."),
                Some(k) => match db.get(k.as_bytes()) {
                    Err(e) => println!("Err: {:?}", e),
                    Ok(Some(v)) => match String::from_utf8(v.to_vec()) {
                        Ok(vv) => println!("{}", vv),
                        Err(_) => println!("\"{:?}\"", v),
                    },
                    Ok(None) => println!("nil"),
                },
            },
            "set" => {
                let (key, value) = (params.next(), params.next());
                if key.is_none() || value.is_none() {
                    println!("key and value should be specified");
                } else {
                    let (key, value) = (key.unwrap(), value.unwrap());
                    if let Err(e) = db.set(key.into(), value.into()) {
                        println!("Err: {:?}", e);
                    }
                }
            }
            "del" => match params.next() {
                None => println!("key should be specified."),
                Some(k) => {
                    if let Err(e) = db.del(k.into()) {
                        println!("Err: {:?}", e);
                    }
                }
            },
            "info" => match db.info() {
                Err(e) => println!("Err: {:?}", e),
                Ok(i) => println!("{}", i),
            },
            "exit" => break,
            "compact" => println!("unimplemented."),
            "scan" => println!("unimplemented."),
            _ => println!("only GET/SET/DEL/SCAN/INFO/COMPACT available."),
        }
    }
}

#[allow(unused)]
fn log_compaction(data: &Any, key: &[u8], seq_num: u64, value: Option<Bytes>) -> bool {
    let tx = data
        .downcast_ref::<Sender<(Vec<u8>, u64, Option<Bytes>)>>()
        .unwrap();
    let _ = tx.send((key.to_vec(), seq_num, value));
    true
}

fn get_opts(a: Any) -> Opts<BytewiseComparator> {
    Arc::new(OptsRaw {
        filter_name: Some("leveldb.BuiltinBloomFilter2".to_string()),
        verify_checksum: false,
        block_cache_limit: 1024 * 1024 * 100,
        block_restart_interval: 16,
        block_size: 4096,
        compression: true,
        comparer: BytewiseComparator::default(),
        error_if_db_exists: false,
        write_buffer_size: 4096000,
        max_file_size: 4 * 1024 * 1024,
        // compact_hook: (a, log_compaction),
        compact_hook: (a, empty_compact_hook),
        flush_wal: false,
    })
}

fn main() {
    let args = std::env::args().collect::<Vec<String>>();
    let subcmd = args[1].clone();
    match subcmd.to_lowercase().as_str() {
        "interactive" => interactive(),
        "learn_file" => match args.get(2) {
            None => return println!("file number should be specified"),
            Some(n) => learn_file(n.as_str()),
        },
        "load" => {
            let filename = match args.get(2) {
                None => "test.data".to_string(),
                Some(v) => v.clone(),
            };
            load(filename);
        }
        _ => unimplemented!(),
    }
    return;
}

fn learn_file(name: &str) {
    let opts = get_opts(Any::new(0));
    let fs = OsFS::default();
    let dirname = format!("{}/data", OsFS::default().pwd().unwrap());
    let full_name = format!("{}/{}", dirname, name);
    let f = fs.open(full_name.as_str()).unwrap();
    let reader = SSTableReader::open(f, 0, &opts).unwrap();
    let mut scanner = SSTableScanner::new(reader, opts.clone());
    loop {
        match scanner.next() {
            Err(e) => {
                println!("Err: {:?}", e);
                std::process::exit(-1);
            }
            Ok(None) => break,
            Ok(Some((k, v))) => println!("{}: {:?}", k, v),
        }
    }
}

fn load(filename: String) {
    let opts = get_opts(Any::new(()));
    let dirname = format!("{}/data", OsFS::default().pwd().unwrap());
    let db: DB<BytewiseComparator, File, BTMap> =
        DB::open(dirname.clone(), opts.clone(), &OS_FS).unwrap();
    let mut f = File::open(filename).expect("file not found");
    let mut buf = String::new();
    f.read_to_string(&mut buf).expect("failed to read file");
    let mut cmds = vec![];
    for (i, line) in buf.split("\n").enumerate() {
        let params = line
            .split(" ")
            .map(|x| x.to_string())
            .collect::<Vec<String>>();
        match params.len() {
            0 => continue,
            1 => cmds.push((params[0].clone(), None)),
            2 => cmds.push((params[0].clone(), Some(params[1].clone()))),
            _ => println!("too many arguments at line {}", i),
        }
    }

    let mut cmd_groups = vec![vec![], vec![], vec![], vec![]];
    let mut i = 0;
    for cmd in cmds.into_iter() {
        cmd_groups[i % 4].push(cmd);
        i += 1;
    }
    let mut threads = vec![];
    for _ in 0..4 {
        let db_c = db.clone();
        let cmds = cmd_groups.pop().unwrap();
        let j = std::thread::spawn(move || {
            for (k, v) in cmds {
                let r = match v {
                    None => db_c.del(k.into()),
                    Some(val) => db_c.set(k.into(), val.into()),
                };
                if let Err(e) = r {
                    println!("failed to write to db: {:?}", e);
                    break;
                }
            }
        });
        threads.push(j);
    }
    for t in threads {
        t.join().unwrap();
    }
}
