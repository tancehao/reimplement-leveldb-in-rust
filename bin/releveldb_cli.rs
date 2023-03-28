extern crate core;

use bytes::Bytes;
use releveldb::compare::BYTEWISE_COMPARATOR;
use releveldb::db::{DBScanner, DB};
use releveldb::io::{OsFS, StorageSystem, OS_FS};
use releveldb::memtable::simple::BTMap;
use releveldb::opts::{Opts, OptsRaw, ReadOptions, WriteOptions};
use releveldb::sstable::reader::SSTableReader;
use releveldb::sstable::reader::SSTableScanner;
use releveldb::utils::crc::crc32;
use releveldb::wal::WalReader;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Arc;

fn interactive() {
    let opts = get_opts();
    let dirname = format!("{}/data", OsFS::default().pwd().unwrap());
    let db: DB<File, BTMap> = DB::open(dirname.clone(), opts.clone(), &OS_FS).unwrap();
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
                Some(k) => {
                    let ro = ReadOptions::default();
                    match db.get(&ro, k.as_bytes()) {
                        Err(e) => println!("Err: {:?}", e),
                        Ok(Some(v)) => match String::from_utf8(v.to_vec()) {
                            Ok(vv) => println!("{}", vv),
                            Err(_) => println!("\"{:?}\"", v),
                        },
                        Ok(None) => println!("nil"),
                    }
                }
            },
            "set" => {
                let (key, value) = (params.next(), params.next());
                if key.is_none() || value.is_none() {
                    println!("key and value should be specified");
                } else {
                    let wo = WriteOptions::default();
                    let (key, value) = (key.unwrap(), value.unwrap());
                    if let Err(e) = db.set(&wo, key.into(), value.into()) {
                        println!("Err: {:?}", e);
                    }
                }
            }
            "del" => match params.next() {
                None => println!("key should be specified."),
                Some(k) => {
                    if let Err(e) = db.del(&WriteOptions::default(), k.into()) {
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

fn get_opts() -> Opts {
    Arc::new(OptsRaw {
        filter_name: Some("leveldb.BuiltinBloomFilter2".to_string()),
        verify_checksum: false,
        block_cache_limit: 1024 * 1024 * 100,
        block_restart_interval: 16,
        block_size: 4096,
        compression: true,
        comparer: BYTEWISE_COMPARATOR,
        error_if_db_exists: false,
        write_buffer_size: 64 * 1024 * 1024,
        max_file_size: 4 * 1024 * 1024,
        flush_wal: false,
        tiered_parallel: true,
        enable_metrics_server: false,
    })
}

fn main() {
    let args = std::env::args().collect::<Vec<String>>();
    let subcmd = args[1].clone();
    match subcmd.to_lowercase().as_str() {
        "interactive" => interactive(),
        "learn_sst" => match args.get(2) {
            None => return println!("file number should be specified"),
            Some(n) => learn_sst(n.as_str()),
        },
        "learn_wal" => match args.get(2) {
            None => panic!("file name of wal should be specified"),
            Some(v) => learn_wal(v.as_str()),
        },
        "load" => {
            let filename = match args.get(2) {
                None => "test.data".to_string(),
                Some(v) => v.clone(),
            };
            load(filename);
        }
        "test_query" => {
            let filename = match args.get(2) {
                None => "test.data".to_string(),
                Some(v) => v.clone(),
            };
            test_query(filename);
        }
        _ => unimplemented!(),
    }
    return;
}

fn learn_sst(name: &str) {
    let opts = get_opts();
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

fn learn_wal(name: &str) {
    let fs = OsFS::default();
    let dirname = format!("{}/data", OsFS::default().pwd().unwrap());
    let full_name = format!("{}/{}", dirname, name);
    let f = fs.open(full_name.as_str()).unwrap();
    let mut reader = WalReader::new(f, "wal".to_string()).unwrap();
    loop {
        match reader.next() {
            Err(e) => panic!("{:?}", e),
            Ok(Some(v)) => println!("{:?}", Bytes::from(v)),
            Ok(None) => break,
        }
    }
}

fn load(filename: String) {
    const THREADS: usize = 4;

    let opts = get_opts();
    let dirname = format!("{}/data", OsFS::default().pwd().unwrap());
    let db: DB<File, BTMap> = DB::open(dirname.clone(), opts.clone(), &OS_FS).unwrap();
    let mut f = File::open(filename).expect("file not found");
    let mut buf = String::new();
    f.read_to_string(&mut buf).expect("failed to read file");
    let mut cmd_groups = vec![vec![]; THREADS];
    for (i, line) in buf.split("\n").enumerate() {
        let params = line
            .split(" ")
            .map(|x| x.to_string())
            .collect::<Vec<String>>();
        let (k, v) = match params.len() {
            0 => continue,
            1 => (params[0].clone(), None),
            2 => (params[0].clone(), Some(params[1].clone())),
            _ => panic!("too many arguments at line {}", i),
        };
        let hash = crc32(k.as_bytes()) as usize;
        cmd_groups[hash % THREADS].push((k, v));
    }
    let mut threads = vec![];
    for _ in 0..THREADS {
        let db_c = db.clone();
        let cmds = cmd_groups.pop().unwrap();
        let j = std::thread::spawn(move || {
            let wo = WriteOptions::default();
            for (k, v) in cmds {
                let r = match v {
                    None => db_c.del(&wo, k.into()),
                    Some(val) => db_c.set(&wo, k.into(), val.into()),
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

fn test_query(filename: String) {
    const THREADS: usize = 8;

    let opts = get_opts();
    let dirname = format!("{}/data", OsFS::default().pwd().unwrap());
    let db: DB<File, BTMap> = DB::open(dirname.clone(), opts.clone(), &OS_FS).unwrap();
    let mut f = File::open(filename).expect("file not found");
    let mut buf = String::new();
    f.read_to_string(&mut buf).expect("failed to read file");
    let mut kvs_groups = vec![HashMap::new(); THREADS];

    for (i, line) in buf.split("\n").enumerate() {
        let params = line
            .split(" ")
            .map(|x| x.to_string())
            .collect::<Vec<String>>();
        let (k, v) = match params.len() {
            0 => continue,
            1 => (params[0].clone(), None),
            2 => (params[0].clone(), Some(params[1].clone())),
            _ => panic!("too many arguments at line {}", i),
        };
        let hash = crc32(k.as_bytes()) as usize;
        kvs_groups[hash % THREADS].insert(k, v);
    }

    let mut threads = vec![];
    for _ in 0..THREADS {
        let db_c = db.clone();
        let ro = ReadOptions::default();
        let kvs = kvs_groups.pop().unwrap();
        let j = std::thread::spawn(move || {
            for (k, v) in kvs.iter() {
                let vv = db_c.get(&ro, k.as_bytes()).unwrap();
                if vv.as_ref() != v.as_ref().map(|x| Bytes::from(x.clone())).as_ref() {
                    println!(
                        "not equal. key: {:?}, expected: {:?}, result: {:?}",
                        k, v, vv
                    );
                } else {
                    println!("equal. key: {:?}", k);
                }
            }
        });
        threads.push(j);
    }
    for t in threads {
        t.join().unwrap();
    }
}
