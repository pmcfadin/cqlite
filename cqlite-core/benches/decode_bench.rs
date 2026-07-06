//! Decode micro-benchmarks for cqlite-core (issue #1615, Epic H / H2).
//!
//! These benches pin the *decode-level* cost that nothing else measures: the
//! existing `read/type_heavy` bench exercises the whole v5 read path (open →
//! seek → chunk decompress → row/cell decode), so a pure decode regression can
//! hide inside its I/O. This target isolates the block-path value decoder.
//!
//! Three criterion groups:
//!
//! - `decode/type_<name>` — for each CQL type (all scalars + `list`/`set`/`map`/
//!   `tuple`/UDT/`frozen`), decode a FIXED representative byte buffer through the
//!   **live** block-path entry `SSTableReader::parse_value_with_schema_type`,
//!   reached via the opt-in `#[doc(hidden)]` `decode_value_for_bench` shim (never
//!   a re-implemented copy). ns/op per type. Numerous + individually noisy, so
//!   these are recorded/advisory, not STRICT perf-gate entries.
//! - `decode/wide_row_primitives` — `Throughput::Elements(rows)` decoding a wide
//!   all-primitive row (~20 primitive columns) repeated to a row volume; rows/sec.
//! - `decode/text_heavy` — `Throughput::Elements(rows)` on a `text`/`blob`-dominated
//!   column set (the K5/K6 measurement); rows/sec.
//!
//! # Why a real opened reader supplies `&self`
//!
//! The scalar decode arms are `self`-independent, but the collection/UDT/tuple/
//! frozen arms recurse via `&self` and read `self.header.cassandra_version`. So the
//! bench opens ONE real, CI-present V5 fixture reader (`SIMPLE`,
//! `test_basic.simple_table`) once, outside every measured region, and reuses it as
//! the decode context. The per-type byte buffers are fixed literals built here, so
//! the measurement is deterministic and independent of any single fixture's columns
//! while still routing every decode through the live entry.
//!
//! # UDT note
//!
//! `parse_value_with_schema_type` resolves its `data_type` string via
//! `ComparatorType::from_data_type`, which does NOT consult a UDT registry, so it
//! can never produce a `ComparatorType::Udt` from a type string — a UDT reference
//! decodes through the `Custom` fallback. The UDT-class decode (multi-field, i32-BE
//! field lengths) is therefore benched via its structural twin `tuple`: Cassandra's
//! `UserType` extends `TupleType` and shares the identical wire format and decode
//! logic (`parse_udt_value` vs `parse_tuple_value`). The `decode/type_udt` entry
//! documents this and asserts `Value::Tuple`.
//!
//! # Gating
//!
//! Needs `cli-helpers` (the `open_read_db`/fixture loader + a queryable reader) AND
//! `bench-internals` (the `decode_value_for_bench` shim). Without BOTH, the bench
//! compiles to an empty-but-valid criterion main (mirrors `read.rs`).

use criterion::{criterion_group, criterion_main};

#[path = "fixtures/mod.rs"]
mod fixtures;

#[path = "bench_ledger/mod.rs"]
mod bench_ledger;

#[path = "profiling/mod.rs"]
mod profiling;

// ---------------------------------------------------------------------------
// Real benches (need BOTH cli-helpers and bench-internals)
// ---------------------------------------------------------------------------

#[cfg(all(feature = "cli-helpers", feature = "bench-internals"))]
mod decode_impl {
    use super::{bench_ledger, fixtures};
    use cqlite_core::storage::sstable::reader::SSTableReader;
    use cqlite_core::{Config, Platform, Value};
    use criterion::{black_box, Criterion, Throughput};
    use std::sync::Arc;

    /// Fixed number of "rows" decoded per throughput iteration (a stable row
    /// volume for the rows/sec measurement; identical across runs and machines).
    const ROWS: u64 = 1_000;

    /// One per-type decode case: a fixed byte buffer, the type string the live
    /// entry resolves, and a matcher proving the decode took the right arm.
    struct TypeCase {
        name: &'static str,
        data_type: String,
        buf: Vec<u8>,
        /// Non-capturing (coerces to `fn`) matcher for the expected `Value`
        /// variant — wiring evidence that the live entry ran, not a no-op.
        matches: fn(&Value) -> bool,
    }

    /// Minimal Cassandra unsigned-VInt length prefix for small values (< 128 fit
    /// in a single byte, which is all the fixed collection buffers here need).
    fn vlen(n: u64) -> Vec<u8> {
        assert!(n < 0x80, "vlen helper only encodes single-byte lengths");
        vec![n as u8]
    }

    /// Assemble a `list`/`set` body: element count then per-element (vlen, bytes).
    fn collection_of_ints(vals: &[i32]) -> Vec<u8> {
        let mut buf = vlen(vals.len() as u64);
        for v in vals {
            buf.extend_from_slice(&vlen(4));
            buf.extend_from_slice(&v.to_be_bytes());
        }
        buf
    }

    /// Assemble a `map<text,text>` body: entry count then per-entry
    /// (vlen(key), key, vlen(val), val).
    fn map_text_text(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut buf = vlen(entries.len() as u64);
        for (k, v) in entries {
            buf.extend_from_slice(&vlen(k.len() as u64));
            buf.extend_from_slice(k.as_bytes());
            buf.extend_from_slice(&vlen(v.len() as u64));
            buf.extend_from_slice(v.as_bytes());
        }
        buf
    }

    /// Assemble a `tuple`/UDT body: per-field 4-byte BE i32 length then field bytes
    /// (Cassandra `TupleType`/`UserType` wire format; -1 = null, unused here).
    fn tuple_int_text(i: i32, s: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&4i32.to_be_bytes());
        buf.extend_from_slice(&i.to_be_bytes());
        buf.extend_from_slice(&(s.len() as i32).to_be_bytes());
        buf.extend_from_slice(s.as_bytes());
        buf
    }

    /// Cassandra DATE wire form for `days` since epoch (offset by i32::MIN for
    /// byte-order comparability); the decoder inverts with `wrapping_add`.
    fn date_bytes(days: i32) -> Vec<u8> {
        (days as u32)
            .wrapping_sub(i32::MIN as u32)
            .to_be_bytes()
            .to_vec()
    }

    /// The full per-type case set routed through the live decode entry.
    fn type_cases() -> Vec<TypeCase> {
        vec![
            TypeCase {
                name: "boolean",
                data_type: "boolean".into(),
                buf: vec![1],
                matches: |v| matches!(v, Value::Boolean(_)),
            },
            TypeCase {
                name: "tinyint",
                data_type: "tinyint".into(),
                buf: vec![42],
                matches: |v| matches!(v, Value::TinyInt(_)),
            },
            TypeCase {
                name: "smallint",
                data_type: "smallint".into(),
                buf: 4200i16.to_be_bytes().to_vec(),
                matches: |v| matches!(v, Value::SmallInt(_)),
            },
            TypeCase {
                name: "int",
                data_type: "int".into(),
                buf: 42i32.to_be_bytes().to_vec(),
                matches: |v| matches!(v, Value::Integer(_)),
            },
            TypeCase {
                name: "bigint",
                data_type: "bigint".into(),
                buf: 42i64.to_be_bytes().to_vec(),
                matches: |v| matches!(v, Value::BigInt(_)),
            },
            TypeCase {
                name: "counter",
                data_type: "counter".into(),
                buf: 7i64.to_be_bytes().to_vec(),
                matches: |v| matches!(v, Value::Counter(_)),
            },
            TypeCase {
                name: "float",
                data_type: "float".into(),
                buf: 1.5f32.to_be_bytes().to_vec(),
                matches: |v| matches!(v, Value::Float32(_)),
            },
            TypeCase {
                name: "double",
                data_type: "double".into(),
                buf: 2.5f64.to_be_bytes().to_vec(),
                matches: |v| matches!(v, Value::Float(_)),
            },
            TypeCase {
                name: "text",
                data_type: "text".into(),
                buf: b"hello".to_vec(),
                matches: |v| matches!(v, Value::Text(_)),
            },
            TypeCase {
                name: "varchar",
                data_type: "varchar".into(),
                buf: b"hello".to_vec(),
                matches: |v| matches!(v, Value::Text(_)),
            },
            TypeCase {
                name: "ascii",
                data_type: "ascii".into(),
                buf: b"hello".to_vec(),
                matches: |v| matches!(v, Value::Text(_)),
            },
            TypeCase {
                name: "blob",
                data_type: "blob".into(),
                buf: vec![0xde, 0xad, 0xbe, 0xef],
                matches: |v| matches!(v, Value::Blob(_)),
            },
            TypeCase {
                name: "timestamp",
                data_type: "timestamp".into(),
                buf: 1_700_000_000_000i64.to_be_bytes().to_vec(),
                matches: |v| matches!(v, Value::Timestamp(_)),
            },
            TypeCase {
                name: "date",
                data_type: "date".into(),
                buf: date_bytes(19_500),
                matches: |v| matches!(v, Value::Date(_)),
            },
            TypeCase {
                name: "time",
                data_type: "time".into(),
                buf: 45_000_000_000i64.to_be_bytes().to_vec(),
                matches: |v| matches!(v, Value::Time(_)),
            },
            TypeCase {
                name: "uuid",
                data_type: "uuid".into(),
                buf: (0u8..16).collect(),
                matches: |v| matches!(v, Value::Uuid(_)),
            },
            TypeCase {
                name: "timeuuid",
                data_type: "timeuuid".into(),
                buf: (0u8..16).collect(),
                matches: |v| matches!(v, Value::Uuid(_)),
            },
            TypeCase {
                name: "inet",
                data_type: "inet".into(),
                buf: vec![10, 0, 0, 1],
                matches: |v| matches!(v, Value::Inet(_)),
            },
            TypeCase {
                name: "varint",
                data_type: "varint".into(),
                buf: vec![0x01, 0x02, 0x03],
                matches: |v| matches!(v, Value::Varint(_)),
            },
            TypeCase {
                name: "decimal",
                // 4-byte scale (=2) + variable-length unscaled value.
                data_type: "decimal".into(),
                buf: {
                    let mut b = 2i32.to_be_bytes().to_vec();
                    b.extend_from_slice(&[0x30, 0x39]); // unscaled 12345
                    b
                },
                matches: |v| matches!(v, Value::Decimal { .. }),
            },
            TypeCase {
                name: "duration",
                // Three ZigZag VInts (months, days, nanos); all zero => three 0x00.
                data_type: "duration".into(),
                buf: vec![0, 0, 0],
                matches: |v| matches!(v, Value::Duration { .. }),
            },
            TypeCase {
                name: "json",
                data_type: "json".into(),
                buf: b"[1,2,3]".to_vec(),
                matches: |v| matches!(v, Value::Json(_)),
            },
            TypeCase {
                name: "list",
                data_type: "list<int>".into(),
                buf: collection_of_ints(&[1, 2, 3]),
                matches: |v| matches!(v, Value::List(_)),
            },
            TypeCase {
                name: "set",
                data_type: "set<int>".into(),
                buf: collection_of_ints(&[10, 20]),
                matches: |v| matches!(v, Value::Set(_)),
            },
            TypeCase {
                name: "map",
                data_type: "map<text,text>".into(),
                buf: map_text_text(&[("k1", "v1"), ("k2", "v2")]),
                matches: |v| matches!(v, Value::Map(_)),
            },
            TypeCase {
                name: "tuple",
                data_type: "tuple<int,text>".into(),
                buf: tuple_int_text(42, "hi"),
                matches: |v| matches!(v, Value::Tuple(_)),
            },
            TypeCase {
                // UDT-class decode via its structural twin `tuple` — see module doc:
                // the string entry cannot construct a `ComparatorType::Udt`.
                name: "udt",
                data_type: "tuple<int,text,int>".into(),
                buf: {
                    let mut b = tuple_int_text(1, "name");
                    b.extend_from_slice(&4i32.to_be_bytes());
                    b.extend_from_slice(&2i32.to_be_bytes());
                    b
                },
                matches: |v| matches!(v, Value::Tuple(_)),
            },
            TypeCase {
                name: "frozen",
                data_type: "frozen<list<int>>".into(),
                buf: collection_of_ints(&[1, 2, 3]),
                matches: |v| matches!(v, Value::Frozen(_)),
            },
        ]
    }

    /// Locate the `-Data.db` component inside the SIMPLE fixture directory.
    fn simple_data_db() -> std::path::PathBuf {
        let dir = fixtures::table_dir(
            fixtures::ReadFixture::SIMPLE.keyspace,
            fixtures::ReadFixture::SIMPLE.table,
        );
        std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("read fixture dir {}: {e}", dir.display()))
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
            .unwrap_or_else(|| {
                panic!(
                    "no -Data.db under {} — fetch fixtures: bash test-data/scripts/fetch-datasets.sh",
                    dir.display()
                )
            })
    }

    /// Open one real SIMPLE reader as the `&self` decode context. Panics with an
    /// actionable message if the fixture is absent (per-type benches require it).
    fn open_simple_reader(rt: &tokio::runtime::Runtime) -> SSTableReader {
        let path = simple_data_db();
        let config = Config::default();
        let platform = Arc::new(
            rt.block_on(Platform::new(&config))
                .expect("init platform for decode bench"),
        );
        rt.block_on(SSTableReader::open(&path, &config, platform))
            .expect("open SIMPLE reader for decode bench")
    }

    /// `decode/type_<name>` — per-CQL-type decode through the live entry.
    pub fn bench_types(c: &mut Criterion) {
        if !fixtures::fixture_present(&fixtures::ReadFixture::SIMPLE) {
            eprintln!("decode/type_*: SIMPLE fixture absent — skip-register (no group)");
            return;
        }
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let reader = open_simple_reader(&rt);
        let cases = type_cases();

        // Wiring evidence: every case must decode through the live entry to the
        // expected variant BEFORE we measure, so a no-op / wrong-path decode
        // fails loudly rather than benching nothing.
        for case in &cases {
            let decoded = reader
                .decode_value_for_bench(&case.buf, &case.data_type)
                .unwrap_or_else(|e| panic!("decode/type_{}: live entry errored: {e}", case.name));
            assert!(
                (case.matches)(&decoded),
                "decode/type_{}: unexpected Value variant {decoded:?} (wrong decode path?)",
                case.name
            );
        }

        let mut group = c.benchmark_group("decode");
        for case in &cases {
            let name = format!("type_{}", case.name);
            group.bench_function(&name, |bch| {
                bch.iter(|| {
                    let v = reader
                        .decode_value_for_bench(black_box(&case.buf), black_box(&case.data_type))
                        .expect("decode value");
                    black_box(v)
                });
            });
        }
        group.finish();
    }

    /// A fixed set of ~20 primitive typed columns (name is documentary only).
    fn wide_primitive_columns() -> Vec<(String, Vec<u8>)> {
        vec![
            ("int".into(), 42i32.to_be_bytes().to_vec()),
            ("int".into(), (-7i32).to_be_bytes().to_vec()),
            ("bigint".into(), 42i64.to_be_bytes().to_vec()),
            ("bigint".into(), 9_000_000_000i64.to_be_bytes().to_vec()),
            ("smallint".into(), 300i16.to_be_bytes().to_vec()),
            ("tinyint".into(), vec![9]),
            ("boolean".into(), vec![1]),
            ("boolean".into(), vec![0]),
            ("float".into(), 1.5f32.to_be_bytes().to_vec()),
            ("double".into(), 2.5f64.to_be_bytes().to_vec()),
            ("uuid".into(), (0u8..16).collect()),
            ("timeuuid".into(), (16u8..32).collect()),
            (
                "timestamp".into(),
                1_700_000_000_000i64.to_be_bytes().to_vec(),
            ),
            ("date".into(), date_bytes(19_500)),
            ("time".into(), 45_000_000_000i64.to_be_bytes().to_vec()),
            ("counter".into(), 5i64.to_be_bytes().to_vec()),
            ("int".into(), 12345i32.to_be_bytes().to_vec()),
            ("bigint".into(), (-1i64).to_be_bytes().to_vec()),
            ("smallint".into(), (-2i16).to_be_bytes().to_vec()),
            ("boolean".into(), vec![1]),
        ]
    }

    /// A `text`/`blob`-dominated column set (K5/K6 measurement).
    fn text_heavy_columns() -> Vec<(String, Vec<u8>)> {
        let text = |s: &str| ("text".to_string(), s.as_bytes().to_vec());
        vec![
            text("alpha"),
            text("bravo"),
            text("charlie the quick brown fox jumped"),
            text("delta"),
            text("echo foxtrot golf hotel"),
            ("varchar".into(), b"india".to_vec()),
            ("ascii".into(), b"juliet".to_vec()),
            ("blob".into(), vec![0u8; 32]),
            ("blob".into(), vec![0xab; 64]),
            text("kilo lima mike november oscar papa"),
        ]
    }

    /// Time `iters` decodes of the full `cols` set and record rows/sec + ns/row to
    /// the unified ledger (best-effort — a ledger failure never aborts the bench).
    fn record_throughput(
        reader: &SSTableReader,
        metric_prefix: &str,
        cols: &[(String, Vec<u8>)],
        iters: u64,
    ) {
        let start = std::time::Instant::now();
        for _ in 0..iters {
            for (dt, buf) in cols {
                let v = reader
                    .decode_value_for_bench(buf, dt)
                    .expect("decode column");
                black_box(v);
            }
        }
        let elapsed = start.elapsed().as_secs_f64();
        if elapsed <= 0.0 {
            return;
        }
        let rows_per_sec = iters as f64 / elapsed;
        let ns_per_row = elapsed * 1e9 / iters as f64;
        let m_rps = format!("{metric_prefix}/rows_per_sec");
        let m_nspr = format!("{metric_prefix}/ns_per_row");
        if let Err(e) = bench_ledger::append_metrics(
            "decode",
            &[
                (m_rps.as_str(), rows_per_sec, "rows_per_sec"),
                (m_nspr.as_str(), ns_per_row, "ns"),
            ],
        ) {
            eprintln!(
                "decode: could not append unified ledger {}: {e}",
                bench_ledger::ledger_path().display()
            );
        }
    }

    /// Decode all `cols` `ROWS` times per iteration; `Throughput::Elements(ROWS)`.
    fn bench_row_set(
        c: &mut Criterion,
        reader: &SSTableReader,
        bench: &str,
        cols: &[(String, Vec<u8>)],
    ) {
        // Wiring evidence + one recorded ledger pass before measuring.
        for (dt, buf) in cols {
            reader
                .decode_value_for_bench(buf, dt)
                .unwrap_or_else(|e| panic!("decode/{bench}: column {dt} errored: {e}"));
        }
        record_throughput(reader, bench, cols, ROWS);

        let mut group = c.benchmark_group("decode");
        group.throughput(Throughput::Elements(ROWS));
        group.bench_function(bench, |bch| {
            bch.iter(|| {
                for _ in 0..ROWS {
                    for (dt, buf) in cols {
                        let v = reader
                            .decode_value_for_bench(black_box(buf), black_box(dt))
                            .expect("decode column");
                        black_box(v);
                    }
                }
            });
        });
        group.finish();
    }

    /// `decode/wide_row_primitives` — rows/sec over a wide all-primitive row.
    pub fn bench_wide_row_primitives(c: &mut Criterion) {
        if !fixtures::fixture_present(&fixtures::ReadFixture::SIMPLE) {
            eprintln!("decode/wide_row_primitives: SIMPLE fixture absent — skip-register");
            return;
        }
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let reader = open_simple_reader(&rt);
        bench_row_set(c, &reader, "wide_row_primitives", &wide_primitive_columns());
    }

    /// `decode/text_heavy` — rows/sec over a text/blob-dominated column set.
    pub fn bench_text_heavy(c: &mut Criterion) {
        if !fixtures::fixture_present(&fixtures::ReadFixture::SIMPLE) {
            eprintln!("decode/text_heavy: SIMPLE fixture absent — skip-register");
            return;
        }
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let reader = open_simple_reader(&rt);
        bench_row_set(c, &reader, "text_heavy", &text_heavy_columns());
    }
}

// ---------------------------------------------------------------------------
// `decode/vint_decode` — the VInt decode primitive micro-bench (issue #1638,
// Epic J / J4). Pure public API over `decode_unsigned` / `decode_signed`: no
// fixture, no reader, so it runs feature-INDEPENDENTLY (registered in BOTH the
// full and no-op criterion mains below).
// ---------------------------------------------------------------------------
pub fn bench_vint_decode(c: &mut criterion::Criterion) {
    use cqlite_core::parser::vint::{decode_signed, decode_unsigned};
    use std::hint::black_box;

    // A fixed table of representative unsigned VInt buffers spanning every
    // encoded width 1..=9 (lengths implied by the leading-ones lead byte).
    let unsigned_buffers: [&[u8]; 9] = [
        &[0x7F],                                                 // 1 byte
        &[0x80, 0x80],                                           // 2 bytes
        &[0xC0, 0x40, 0x00],                                     // 3 bytes
        &[0xE0, 0x10, 0x00, 0x00],                               // 4 bytes
        &[0xF0, 0x08, 0x00, 0x00, 0x00],                         // 5 bytes
        &[0xF8, 0x04, 0x00, 0x00, 0x00, 0x00],                   // 6 bytes
        &[0xFC, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00],             // 7 bytes
        &[0xFE, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],       // 8 bytes
        &[0xFF, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], // 9 bytes
    ];

    let mut group = c.benchmark_group("decode");
    group.bench_function("vint_decode", |b| {
        b.iter(|| {
            for buf in unsigned_buffers.iter() {
                let (v, n) = decode_unsigned(black_box(buf)).expect("valid unsigned vint");
                black_box((v, n));
                let (s, m) = decode_signed(black_box(buf)).expect("valid signed vint");
                black_box((s, m));
            }
        })
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// criterion_group! / criterion_main! — gated so the target builds an empty but
// valid main without BOTH cli-helpers and bench-internals (mirrors read.rs).
// The feature-independent `bench_vint_decode` is registered in BOTH.
// ---------------------------------------------------------------------------

#[cfg(all(feature = "cli-helpers", feature = "bench-internals"))]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = decode_impl::bench_types,
              decode_impl::bench_wide_row_primitives,
              decode_impl::bench_text_heavy,
              bench_vint_decode
);

#[cfg(not(all(feature = "cli-helpers", feature = "bench-internals")))]
fn bench_noop(_c: &mut criterion::Criterion) {
    // Without both cli-helpers and bench-internals there is no reader/shim to
    // drive the fixture-backed groups; the target still compiles and runs.
    eprintln!(
        "decode: fixture-backed groups require --features cli-helpers,bench-internals. \
         Run: cargo bench -p cqlite-core --features cli-helpers,bench-internals --bench decode"
    );
}

#[cfg(not(all(feature = "cli-helpers", feature = "bench-internals")))]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_noop, bench_vint_decode
);

criterion_main!(benches);
