//! Builds the initial corpus: `gen_seeds <corpus_dir>` writes `<corpus_dir>/<target>/<seed>`.
//! Chunk / WAL / MANIFEST seeds are real files produced by a table; the rest come from
//! `moosedb_core::fuzz_api::seeds()`.

use std::fs;
use std::path::{Path, PathBuf};

use moosedb_core::fuzz_api::{seed_schema, seeds};
use moosedb_core::{RawOptions, Table, TableConfig, Value};

fn put(root: &Path, target: &str, name: &str, bytes: &[u8]) {
    let d = root.join(target);
    fs::create_dir_all(&d).unwrap();
    fs::write(d.join(name), bytes).unwrap();
}

fn row(i: i64, host: &str) -> Vec<Value> {
    vec![
        Value::Timestamp(1_790_000_000_000_000 + i * 1_000_000),
        Value::Bytes(host.as_bytes().to_vec()),
        Value::Bytes(b"cpu".to_vec()),
        Value::Float64(20.0 + (i as f64 * 0.3).sin()),
        Value::Int(i),
        if i % 3 == 0 { Value::Null } else { Value::Bytes(format!("n{i}").into_bytes()) },
    ]
}

fn files(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(dir).unwrap().filter_map(|e| e.ok()).map(|e| e.path()).collect();
    v.retain(|p| p.to_string_lossy().ends_with(ext));
    v.sort();
    v
}

fn main() {
    let root = PathBuf::from(std::env::args().nth(1).expect("usage: gen_seeds <corpus_dir>"));
    for (target, name, bytes) in seeds() {
        put(&root, target, &name, &bytes);
    }
    for (label, compression) in [("none", "NONE"), ("lz4", "LZ4"), ("zstd", "ZSTD")] {
        let tmp = std::env::temp_dir().join(format!("moosedb-seeds-{}-{label}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        let opts = RawOptions { compression: Some(compression), ..Default::default() };
        let cfg = TableConfig::new(&tmp, seed_schema(), &opts).unwrap();
        Table::create(&cfg).unwrap();
        {
            let t = Table::open(cfg).unwrap();
            for i in 0..200 {
                t.write(row(i, if i % 2 == 0 { "h1" } else { "h2" })).unwrap();
            }
            t.flush().unwrap();
            for i in 200..230 {
                t.write(row(i, "h3")).unwrap(); // stays in the WAL
            }
            t.sync_wal(true).unwrap();
            for p in files(t.dir(), ".tfl") {
                put(&root, "chunk_file", &format!("{label}_{}", p.file_name().unwrap().to_string_lossy()), &fs::read(p).unwrap());
            }
            for p in files(t.dir(), ".wal") {
                put(&root, "wal_segment", &format!("{label}_{}", p.file_name().unwrap().to_string_lossy()), &fs::read(&p).unwrap());
            }
            if let Ok(m) = fs::read(t.dir().join("MANIFEST")) {
                put(&root, "manifest", &format!("{label}_v2"), &m);
            }
        }
        let _ = fs::remove_dir_all(&tmp);
    }
}
