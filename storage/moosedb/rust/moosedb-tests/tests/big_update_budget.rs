//! An UPDATE that would pass the global batch budget is refused (nothing applied),
//! so many big UPDATEs cannot blow RAM. Own test binary: the budget is process-wide.

#![forbid(unsafe_code)]

use std::sync::Arc;

use moosedb_core::batch::batch_memory_in_use;
use moosedb_core::{settings, Column, ColumnType, Error, RawOptions, Row, Schema, Table, TableConfig, Value};

fn schema() -> Schema {
    Schema::new(
        vec![Column { name: "ts".into(), ty: ColumnType::Timestamp }, Column { name: "v".into(), ty: ColumnType::Float64 }],
        0,
    )
    .unwrap()
}

fn row(v: usize) -> Row {
    vec![Value::Timestamp(1_800_000_000_000_000), Value::Float64(v as f64)]
}

#[test]
fn updates_stop_at_the_global_budget_and_give_it_back() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = TableConfig::new(dir.path().join("t"), schema(), &RawOptions::default()).unwrap();
    Table::create(&cfg).unwrap();
    let t = Arc::new(Table::open(cfg).unwrap());
    t.write(row(0)).unwrap();
    let budget: u64 = 4 << 20; // far under 4 x 64 MiB
    settings::get().set_batch_memory_budget_bytes(budget);
    let mut first = t.begin_batch().unwrap();
    let mut second = t.begin_batch().unwrap();
    let (mut n1, mut n2) = (0usize, 0usize);
    let e1 = loop {
        match first.update(row(0), row(n1 + 1)) {
            Ok(()) => n1 += 1,
            Err(e) => break e,
        }
    };
    assert!(matches!(&e1, Error::Unsupported(m) if m.contains("budget")), "{e1}");
    assert!(batch_memory_in_use() <= budget && n1 > 1000, "{n1}");
    // The budget is spent: the other statement still gets its first MiB, then stops.
    let e2 = loop {
        match second.update(row(0), row(n2 + 1)) {
            Ok(()) => n2 += 1,
            Err(e) => break e,
        }
    };
    assert!(matches!(&e2, Error::Unsupported(m) if m.contains("budget")), "{e2}");
    assert!(n2 > 0 && n2 < n1, "{n2}");
    drop(first); // gives its share back
    drop(second);
    assert_eq!(batch_memory_in_use(), 0);
    let mut second = t.begin_batch().unwrap();
    n2 = 0;
    assert_eq!(batch_memory_in_use(), 0);
    while second.update(row(0), row(n2 + 1)).is_ok() {
        n2 += 1;
    }
    assert!(n2 > 1000, "{n2}");
    drop(second);
    assert_eq!(batch_memory_in_use(), 0);
}
