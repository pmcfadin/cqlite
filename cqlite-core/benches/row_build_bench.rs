//! Row-conversion (row-build) CPU micro-benchmarks (issue #3027; the missing
//! instrument that blocked issue #2901).
//!
//! # Why this target exists
//!
//! Issue #2901 was blocked because **#1883 ran no row-conversion benchmark**, so
//! the projected ~1.04x speedup of a `HashMap`-construction change stayed a
//! projection: nothing in the tree measured
//! [`build_row_from_scan_cached`][bs] — the per-row `(RowKey, ScanRow) →
//! `QueryRow`` conversion every scan pays once per emitted row. The existing
//! `read/*` and `decode/*` benches measure the layers *below* it (open → seek →
//! chunk decompress → cell decode); a row-conversion regression hides inside
//! their I/O. This target isolates the conversion itself.
//!
//! [bs]: cqlite_core::query::build_row_from_scan_cached
//!
//! # What is measured
//!
//! Two criterion groups, both parameterized over the column count `N ∈ {8, 16,
//! 32, 64}` and both reporting **rows/sec** via `Throughput::Elements(2_000)`:
//!
//! - **`row_build/columns_<N>`** — the REAL public function
//!   `build_row_from_scan_cached(key, row, &[] /* SELECT * */, Some(&schema),
//!   &mut pk_cache)`, driven over 2,000 prepared rows through ONE shared
//!   [`PartitionKeyCache`], exactly as the scan loop calls it. This is the whole
//!   per-row cost: map pre-size, `Arc<str>` name moves, `Value::into_owned()`
//!   retention copies, and the memoized partition-key reconstruct.
//! - **`row_build/hashonly_<N>`** — the SAME prepared names/values, but the timed
//!   routine does ONLY `HashMap::with_capacity(n)` + `n` inserts. No partition-key
//!   reconstruct, no `into_owned()`, no projection closure. This isolates the
//!   *map construction + SipHash* term; `columns_<N> − hashonly_<N>` is everything
//!   else in the conversion.
//!
//!   **Do NOT read `hashonly_<N>` as the upside of a hasher swap (#2901).** It bounds
//!   map construction AND hashing together — `HashMap::with_capacity`, the hashbrown
//!   probe/insert, and the map dealloc are all inside it, and a hasher swap removes
//!   NONE of them. Only the SipHash sub-term is addressable that way. For that,
//!   use in-situ profile attribution (`sip::Hasher::write` + `RandomState::hash_one`),
//!   which issue #3027 measured at 4.94% of on-CPU on `read/full_scan`.
//!
//! The bench is **NOT** a `benches/perf-gate.json` entry: it is an instrument, and
//! its absolute rows/sec is machine-dependent. Its value is the ratio between the
//! two groups (and between column counts) on ONE machine, in ONE run.
//!
//! # Fidelity of the synthetic input
//!
//! No dataset fixture is used — the input is fully synthetic so the measurement is
//! independent of any corpus (and so it runs without `CQLITE_DATASETS_ROOT`). The
//! synthesis deliberately reproduces the shape the V5 decoder hands to the
//! conversion:
//!
//! - **Column names** are realistic Cassandra-style identifiers 16–19 chars long
//!   (`col_metric_value_07`, `col_session_uuid_12`, …), interned ONCE into shared
//!   `Arc<str>` handles — name LENGTH is what SipHash consumes, so `a`/`b`/`c`
//!   would understate the map term.
//! - **Values** cycle through a realistic mix — `text` (24 B), `int`, `bigint`,
//!   `uuid`, `timestamp`, `double`, `boolean`. Because `kind_at(i) = KINDS[i % 7]`
//!   restarts on the 24 B `text` arm, per-row payload is NOT a flat multiple of N:
//!   it is **93 B at N=8, 166 B at N=16, 328 B at N=32, 645 B at N=64** (so the
//!   ~690 B/row target shape lands at N=64). The exact figure is printed at setup.
//! - **Text payloads are slices of ONE shared backing `Bytes`**, mimicking a
//!   decompressed chunk. That matters: `Value::into_owned()` (the #1644 D2
//!   retention boundary inside the conversion) copies a shared payload, so a
//!   privately-allocated text value would measure a cheaper path than production.
//! - **Rows are partition-grouped, 10 rows per partition** (200 partitions over
//!   the 2,000 rows), so the `PartitionKeyCache` sees the production-realistic
//!   9-hits-per-miss ratio rather than an all-hit or all-miss extreme.
//!
//! # What is and is not inside the timed region
//!
//! **Input construction is OUTSIDE.** `build_row_from_scan_cached` CONSUMES its
//! `ScanRow`, so each measured iteration needs fresh inputs; they are rebuilt by
//! `iter_batched`'s **untimed setup** closure (`BatchSize::LargeInput`). No input
//! clone is ever measured.
//!
//! **Each produced row is consumed and dropped INSIDE, one at a time.** The
//! routine accumulates a cheap `values.len()` checksum and lets each `QueryRow`
//! drop before building the next, so the live working set is one row — a streamed
//! scan's lifetime. The obvious alternative (push all 2,000 outputs into a `Vec`
//! so their drops fall outside the timer) was measured and REJECTED: retaining the
//! batch makes the timed region touch ~14 MB of fresh map memory at N=64, and that
//! memory-bandwidth cliff swamps the CPU term this instrument exists to isolate.
//! Measured on the same machine, retaining the batch cost 2 790 ns/row for
//! `hashonly_64` vs 1 869 ns/row when dropping per row (+49%), and broke the
//! linear scaling — it read out as "map construction = 98% of the conversion at
//! N=64", an artifact, where the per-row-drop design reads 57–80% across all four
//! column counts. The trade is that map DEALLOCATION is now inside the
//! measurement; that is real per-row work the conversion's caller pays either way,
//! and both groups pay it identically, so the `columns_<N> − hashonly_<N>` delta
//! stays clean.
//!
//! **Wiring guard over the WHOLE pass.** The checksum both routines return must
//! equal `2_000 × (N + 1)`, asserted once per bench id, so a pass that dropped or
//! short-built rows fails loudly instead of measuring less work than advertised.
//!
//! # Determinism
//!
//! Every name, value, and key is a pure function of `(row_index, column_index)` —
//! no wall-clock input, no RNG (seeded or otherwise), so the INPUT is byte-identical
//! on every run and machine. There are no wall-clock threshold asserts anywhere; the
//! only `Instant` use is the best-effort ledger pass, which RECORDS a number and
//! never asserts on one.
//!
//! The *hashing* is not equally reproducible: `RandomState` seeds per process, so
//! collision patterns differ run to run. Both groups share the seed within a run, so
//! the `columns` ∷ `hashonly` ratio is unaffected — but this plausibly contributes to
//! the spread below.
//!
//! # KNOWN LIMITATION — read before quoting a number (issue #3048)
//!
//! Run-to-run spread on an idle box is ~2–3% at N=8/16 but **7–26% at N=32/64**
//! (measured: two runs of the IDENTICAL binary gave `columns_32` 771k vs 722k and
//! `columns_64` 300k vs 379k; the `hashonly_*` control, which no `QueryRow` hasher
//! change can affect, moved 8–25%). The noise tracks memory footprint. **Trust
//! N=8/16; treat N=32/64 as indicative** until #3048 stabilises it. This instrument
//! cannot currently resolve a <10% effect at realistic column counts.
//!
//! # Fidelity caveat
//!
//! The synthetic rows carry no clustering columns and no primary-key pseudo-cells.
//! The real V5 decoder surfaces PK columns into the cell map as pseudo-cells
//! (`row_decoder/mod.rs:719-723`), so in production the partition-key insert is
//! often an OVERWRITE into an N-entry map rather than an (N+1)-th insert. That does
//! not move the ratio, but it means the absolute per-row cost here is not a
//! production figure.
//!
//! # Gating
//!
//! Needs only `state_machine` (the feature that gates `cqlite_core::query`, where
//! `build_row_from_scan_cached` + `PartitionKeyCache` are re-exported) — it is a
//! default feature. `fixtures/mod.rs` is deliberately NOT included: this target
//! opens no SSTable and needs no RNG, so including it would only add compile
//! cost. Without `state_machine` the target compiles to an empty-but-valid
//! criterion main (mirrors `read.rs` / `decode_bench.rs`).
//!
//! Reproduce:
//! ```text
//! cargo bench -p cqlite-core --bench row_build
//! ```

use criterion::{criterion_group, criterion_main};

#[path = "bench_ledger/mod.rs"]
mod bench_ledger;

#[path = "profiling/mod.rs"]
mod profiling;

// ---------------------------------------------------------------------------
// Real benches (need `state_machine` for cqlite_core::query)
// ---------------------------------------------------------------------------

#[cfg(feature = "state_machine")]
mod row_build_impl {
    use super::bench_ledger;
    use bytes::Bytes;
    use cqlite_core::query::{build_row_from_scan_cached, PartitionKeyCache, QueryRow};
    use cqlite_core::schema::{Column, KeyColumn, TableSchema};
    use cqlite_core::types::{RowKey, ScanRow, Value};
    use criterion::{black_box, BatchSize, Criterion, Throughput};
    use std::collections::HashMap;
    use std::sync::Arc;

    /// The map type the `hashonly_<N>` group constructs. It MUST be the same type
    /// `QueryRow::values` is — see [`hashonly_map_type_is_pinned_to_query_row`].
    type ValuesMap = HashMap<Arc<str>, Value>;

    /// **Compile-time pin (issue #3027).** `hashonly_<N>` is only interpretable if
    /// it builds the SAME map type — and therefore runs the SAME hasher — as the
    /// real conversion: if `QueryRow::values` switched hasher (e.g. SipHash →
    /// FxHash) while [`ValuesMap`] did not, the two groups would hash differently
    /// and `columns_<N> − hashonly_<N>` would be meaningless while still producing
    /// confident-looking numbers.
    ///
    /// This function does nothing at runtime; it exists so that such a change
    /// BREAKS THE BUILD here (a loud, actionable compile error) instead of
    /// silently invalidating the instrument. If it fails to compile, update
    /// [`ValuesMap`] to `QueryRow::values`' new type — do not delete this pin.
    #[allow(dead_code)]
    fn hashonly_map_type_is_pinned_to_query_row(row: QueryRow) -> ValuesMap {
        row.values
    }

    /// Rows converted per measured iteration (the `Throughput::Elements` unit, so
    /// criterion reports rows/sec directly).
    const ROWS: usize = 2_000;

    /// Rows per partition — 10 gives the shared [`PartitionKeyCache`] the
    /// realistic 9 hits per miss (200 distinct partitions across [`ROWS`]).
    const ROWS_PER_PARTITION: usize = 10;

    /// Regular-column counts benched (the `_<N>` suffix of each bench id).
    const COLUMN_COUNTS: [usize; 4] = [8, 16, 32, 64];

    /// Byte length of every synthesized `text` payload.
    const TEXT_LEN: usize = 24;

    /// The single TEXT partition-key column. Single-component ⇒ the raw row key
    /// bytes ARE the value (no length prefix), which is the common Cassandra
    /// shape and the one `decode_partition_key_columns` handles on the hot path.
    const PK_COLUMN: &str = "pk_device_id";

    /// The repeating value mix applied across a row's columns (`col_index % 7`).
    #[derive(Clone, Copy)]
    enum Kind {
        Text,
        Int,
        BigInt,
        Uuid,
        Timestamp,
        Double,
        Boolean,
    }

    const KINDS: [Kind; 7] = [
        Kind::Text,
        Kind::Int,
        Kind::BigInt,
        Kind::Uuid,
        Kind::Timestamp,
        Kind::Double,
        Kind::Boolean,
    ];

    impl Kind {
        /// The CQL type string recorded in the schema for this kind.
        fn cql_type(self) -> &'static str {
            match self {
                Kind::Text => "text",
                Kind::Int => "int",
                Kind::BigInt => "bigint",
                Kind::Uuid => "uuid",
                Kind::Timestamp => "timestamp",
                Kind::Double => "double",
                Kind::Boolean => "boolean",
            }
        }

        /// On-wire payload width, used only to report the per-row average.
        fn payload_bytes(self) -> usize {
            match self {
                Kind::Text => TEXT_LEN,
                Kind::Int => 4,
                Kind::BigInt => 8,
                Kind::Uuid => 16,
                Kind::Timestamp => 8,
                Kind::Double => 8,
                Kind::Boolean => 1,
            }
        }

        /// A realistic Cassandra-style column name (16–19 chars) for position `i`.
        fn column_name(self, i: usize) -> String {
            match self {
                Kind::Text => format!("col_label_text_{i:02}"),
                Kind::Int => format!("col_metric_count_{i:02}"),
                Kind::BigInt => format!("col_event_seqno_{i:02}"),
                Kind::Uuid => format!("col_session_uuid_{i:02}"),
                Kind::Timestamp => format!("col_updated_at_{i:02}"),
                Kind::Double => format!("col_metric_value_{i:02}"),
                Kind::Boolean => format!("col_is_active_{i:02}"),
            }
        }
    }

    /// The kind at column position `i`.
    fn kind_at(i: usize) -> Kind {
        KINDS[i % KINDS.len()]
    }

    /// A `TEXT_LEN`-byte deterministic text payload for `(row, col)`.
    fn text_payload(row: usize, col: usize) -> String {
        format!("row{row:06}-col{col:02}-textvabc")
    }

    /// The raw partition-key bytes for partition `p` (a realistic device id).
    fn partition_key_bytes(p: usize) -> Vec<u8> {
        format!("device-{p:06}-region-west").into_bytes()
    }

    /// A prepared, fully synthetic workload for one column count.
    struct Prepared {
        columns: usize,
        schema: TableSchema,
        /// The conversion inputs, partition-grouped.
        rows: Vec<(RowKey, ScanRow)>,
        /// The same cells PLUS a pre-built partition-key entry, as flat
        /// `(name, value)` pairs — the input to the `hashonly_<N>` group.
        hash_rows: Vec<Vec<(Arc<str>, Value)>>,
        /// Mean cell payload bytes per row (documentary; printed at setup).
        avg_payload_bytes: usize,
    }

    /// Build the schema: 1 TEXT partition key + `columns` regular columns.
    fn build_schema(columns: usize) -> TableSchema {
        TableSchema {
            keyspace: "bench_ks".to_string(),
            table: "row_build".to_string(),
            partition_keys: vec![KeyColumn {
                name: PK_COLUMN.to_string(),
                data_type: "text".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: (0..columns)
                .map(|i| {
                    let kind = kind_at(i);
                    Column {
                        name: kind.column_name(i),
                        data_type: kind.cql_type().to_string(),
                        nullable: true,
                        default: None,
                        is_static: false,
                    }
                })
                .collect(),
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    /// Synthesize the whole workload for one column count.
    ///
    /// Text payloads are carved out of ONE shared backing `Bytes` (a stand-in for
    /// a decompressed chunk) so the conversion's `Value::into_owned()` takes the
    /// production copy path rather than the cheap already-tight path.
    fn prepare(columns: usize) -> Prepared {
        let schema = build_schema(columns);

        // Intern each column name ONCE, exactly as the V5 decoder does.
        let names: Vec<Arc<str>> = (0..columns)
            .map(|i| Arc::<str>::from(kind_at(i).column_name(i).as_str()))
            .collect();
        let pk_name: Arc<str> = Arc::<str>::from(PK_COLUMN);

        // One shared backing buffer holding every text payload back-to-back.
        let text_cols: Vec<usize> = (0..columns)
            .filter(|i| matches!(kind_at(*i), Kind::Text))
            .collect();
        let mut backing = Vec::with_capacity(ROWS * text_cols.len() * TEXT_LEN);
        for row in 0..ROWS {
            for col in &text_cols {
                let s = text_payload(row, *col);
                assert_eq!(
                    s.len(),
                    TEXT_LEN,
                    "row_build: synthesized text payload must be exactly {TEXT_LEN} bytes \
                     (got {} for row {row} col {col}) — the payload-size accounting and the \
                     into_owned() copy path both depend on it",
                    s.len()
                );
                backing.extend_from_slice(s.as_bytes());
            }
        }
        let backing = Bytes::from(backing);

        let mut rows = Vec::with_capacity(ROWS);
        let mut hash_rows = Vec::with_capacity(ROWS);
        let mut text_slot = 0usize;
        for row in 0..ROWS {
            let partition = row / ROWS_PER_PARTITION;
            let key = RowKey::new(partition_key_bytes(partition));
            let mut cells: Vec<(Arc<str>, Value)> = Vec::with_capacity(columns);
            for (col, name) in names.iter().enumerate() {
                let value = match kind_at(col) {
                    Kind::Text => {
                        let start = text_slot * TEXT_LEN;
                        text_slot += 1;
                        // A slice of the shared chunk: `into_owned()` must copy it.
                        Value::Text(backing.slice(start..start + TEXT_LEN))
                    }
                    Kind::Int => Value::Integer((row * 31 + col) as i32),
                    Kind::BigInt => Value::BigInt((row as i64) * 1_000_003 + col as i64),
                    Kind::Uuid => {
                        let mut b = [0u8; 16];
                        for (j, slot) in b.iter_mut().enumerate() {
                            *slot = ((row + col * 7 + j) % 251) as u8;
                        }
                        Value::Uuid(b)
                    }
                    Kind::Timestamp => {
                        Value::Timestamp(1_700_000_000_000 + (row as i64) * 1_000 + col as i64)
                    }
                    Kind::Double => Value::Float((row as f64) + (col as f64) / 64.0),
                    Kind::Boolean => Value::Boolean((row + col) % 2 == 0),
                };
                cells.push((Arc::clone(name), value));
            }

            // `hashonly` input: the same interned names + values, plus the
            // partition-key entry PRE-BUILT (no reconstruct from the raw key), so
            // its map ends up with the same N+1 entries and the same capacity hint
            // as the real conversion's — isolating map construction + SipHash.
            let mut flat = cells.clone();
            flat.push((
                Arc::clone(&pk_name),
                Value::text(String::from_utf8_lossy(&partition_key_bytes(partition))),
            ));
            hash_rows.push(flat);

            rows.push((key, ScanRow::Row(cells)));
        }

        let avg_payload_bytes = (0..columns).map(|i| kind_at(i).payload_bytes()).sum();

        Prepared {
            columns,
            schema,
            rows,
            hash_rows,
            avg_payload_bytes,
        }
    }

    /// Setup guard: the REAL conversion must produce a non-empty `QueryRow` with
    /// exactly `columns + 1` values (the N regular cells plus the reconstructed
    /// partition-key column), and that PK column must actually be present. A
    /// silently-empty or PK-less row would mean the bench measures a no-op.
    fn assert_conversion_is_real(p: &Prepared) {
        let (key, row) = p
            .rows
            .first()
            .unwrap_or_else(|| panic!("row_build/columns_{}: no prepared rows", p.columns));
        let mut cache = PartitionKeyCache::default();
        let built =
            build_row_from_scan_cached(key.clone(), row.clone(), &[], Some(&p.schema), &mut cache)
                .unwrap_or_else(|| {
                    panic!(
                "row_build/columns_{}: build_row_from_scan_cached returned None for a LIVE \
                 ScanRow::Row — the bench would measure a suppressed (tombstone) path",
                p.columns
            )
                });
        assert!(
            !built.values.is_empty(),
            "row_build/columns_{}: converted row has zero values — measuring a no-op",
            p.columns
        );
        assert_eq!(
            built.values.len(),
            p.columns + 1,
            "row_build/columns_{}: expected {} values ({} regular cells + 1 reconstructed \
             partition-key column), got {} — the synthesized cells or the schema's \
             partition key disagree with the conversion",
            p.columns,
            p.columns + 1,
            p.columns,
            built.values.len()
        );
        assert_eq!(
            built.values.get(PK_COLUMN),
            Some(&Value::text("device-000000-region-west".to_string())),
            "row_build/columns_{}: partition-key column `{PK_COLUMN}` was not reconstructed \
             from the raw row key — the measured path is missing the PK decode",
            p.columns
        );
    }

    /// Setup guard for the map-only group: the isolated routine must build a map
    /// with the same `columns + 1` entries the real conversion produces.
    fn assert_hashonly_is_real(p: &Prepared) {
        let first = p
            .hash_rows
            .first()
            .unwrap_or_else(|| panic!("row_build/hashonly_{}: no prepared rows", p.columns));
        let mut map: ValuesMap = ValuesMap::with_capacity(first.len());
        for (name, value) in first.iter() {
            map.insert(Arc::clone(name), value.clone());
        }
        assert_eq!(
            map.len(),
            p.columns + 1,
            "row_build/hashonly_{}: map holds {} entries, expected {} — duplicate column \
             names would make this measure fewer inserts than the real conversion",
            p.columns,
            map.len(),
            p.columns + 1
        );
    }

    /// Convert every prepared row through the REAL public function with ONE
    /// shared cache, consuming and dropping each `QueryRow` before the next (see
    /// the module doc: a streamed row lifetime, NOT a retained 2,000-row batch).
    ///
    /// Returns the summed value count across the pass — a cheap checksum that
    /// (a) keeps the optimizer from eliding the conversion and (b) lets the caller
    /// assert EVERY row produced `columns + 1` values, not just the sampled one.
    fn convert_all(
        batch: Vec<(RowKey, ScanRow)>,
        schema: &TableSchema,
        cache: &mut PartitionKeyCache,
    ) -> usize {
        let mut values_seen = 0usize;
        for (key, row) in batch {
            if let Some(built) = build_row_from_scan_cached(key, row, &[], Some(schema), cache) {
                let built = black_box(built);
                values_seen += built.values.len();
                // `built` drops here — one row's worth of live memory at a time.
            }
        }
        values_seen
    }

    /// Build the value map for every prepared row — map construction + SipHash
    /// ONLY (no partition-key reconstruct, no `into_owned()`, no projection).
    /// Same per-row consume-and-drop lifetime and same checksum contract as
    /// [`convert_all`], so the two groups' timed regions differ ONLY by the work
    /// this one omits.
    fn hashmaps_all(batch: Vec<Vec<(Arc<str>, Value)>>) -> usize {
        let mut values_seen = 0usize;
        for row in batch {
            let mut map: ValuesMap = ValuesMap::with_capacity(row.len());
            for (name, value) in row {
                map.insert(name, value);
            }
            let map = black_box(map);
            values_seen += map.len();
            // `map` drops here — one row's worth of live memory at a time.
        }
        values_seen
    }

    /// The checksum both routines must return for a full pass: every one of the
    /// [`ROWS`] rows contributes `columns + 1` values.
    fn expected_checksum(columns: usize) -> usize {
        ROWS * (columns + 1)
    }

    /// Whole-pass wiring guard: a pass that converted fewer values than
    /// [`expected_checksum`] silently measured dropped/short rows.
    fn assert_full_pass(bench_id: &str, columns: usize, checksum: usize) {
        assert_eq!(
            checksum,
            expected_checksum(columns),
            "row_build/{bench_id}: pass produced {checksum} column values, expected {} \
             ({ROWS} rows x {} values) — rows were dropped or built short, so the \
             measurement is not the full workload",
            expected_checksum(columns),
            columns + 1
        );
    }

    /// Best-effort append of one recorded pass to the unified perf ledger. Timing
    /// here is a RECORDED number only — never an assertion (no wall-clock gate).
    fn record_pass(bench_id: &str, elapsed: std::time::Duration) {
        let secs = elapsed.as_secs_f64();
        if secs <= 0.0 {
            return;
        }
        let rows_per_sec = ROWS as f64 / secs;
        let ns_per_row = secs * 1e9 / ROWS as f64;
        let m_rps = format!("{bench_id}/rows_per_sec");
        let m_nspr = format!("{bench_id}/ns_per_row");
        if let Err(e) = bench_ledger::append_metrics(
            "row_build",
            &[
                (m_rps.as_str(), rows_per_sec, "rows_per_sec"),
                (m_nspr.as_str(), ns_per_row, "ns"),
            ],
        ) {
            eprintln!(
                "row_build: could not append unified ledger {}: {e}",
                bench_ledger::ledger_path().display()
            );
        }
    }

    /// `row_build/columns_<N>` — the real conversion, rows/sec.
    pub fn bench_columns(c: &mut Criterion) {
        let mut group = c.benchmark_group("row_build");
        group.throughput(Throughput::Elements(ROWS as u64));
        for columns in COLUMN_COUNTS {
            let p = prepare(columns);
            assert_conversion_is_real(&p);
            let bench_id = format!("columns_{columns}");
            eprintln!(
                "row_build/{bench_id}: {ROWS} rows, {columns} regular columns + 1 PK, \
                 {} rows/partition, ~{} payload bytes/row",
                ROWS_PER_PARTITION, p.avg_payload_bytes
            );

            // One recorded pass for the longitudinal ledger (inputs cloned first,
            // outside the timed span). Doubles as the whole-pass wiring guard.
            {
                let batch = p.rows.clone();
                let mut cache = PartitionKeyCache::default();
                let start = std::time::Instant::now();
                let checksum = convert_all(batch, &p.schema, &mut cache);
                let elapsed = start.elapsed();
                assert_full_pass(&bench_id, columns, checksum);
                record_pass(&bench_id, elapsed);
            }

            // ONE shared cache across the whole measurement, as the scan loop does.
            let mut cache = PartitionKeyCache::default();
            group.bench_function(&bench_id, |bch| {
                bch.iter_batched(
                    || p.rows.clone(),
                    |batch| convert_all(batch, &p.schema, &mut cache),
                    BatchSize::LargeInput,
                );
            });
        }
        group.finish();
    }

    /// `row_build/hashonly_<N>` — map construction + SipHash only, rows/sec.
    pub fn bench_hashonly(c: &mut Criterion) {
        let mut group = c.benchmark_group("row_build");
        group.throughput(Throughput::Elements(ROWS as u64));
        for columns in COLUMN_COUNTS {
            let p = prepare(columns);
            assert_hashonly_is_real(&p);
            let bench_id = format!("hashonly_{columns}");

            {
                let batch = p.hash_rows.clone();
                let start = std::time::Instant::now();
                let checksum = hashmaps_all(batch);
                let elapsed = start.elapsed();
                assert_full_pass(&bench_id, columns, checksum);
                record_pass(&bench_id, elapsed);
            }

            group.bench_function(&bench_id, |bch| {
                bch.iter_batched(|| p.hash_rows.clone(), hashmaps_all, BatchSize::LargeInput);
            });
        }
        group.finish();
    }
}

// ---------------------------------------------------------------------------
// criterion_group! / criterion_main! — gated so the target builds an empty but
// valid main without `state_machine` (mirrors read.rs / decode_bench.rs).
// ---------------------------------------------------------------------------

#[cfg(feature = "state_machine")]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = row_build_impl::bench_columns, row_build_impl::bench_hashonly
);

#[cfg(not(feature = "state_machine"))]
fn bench_noop(_c: &mut criterion::Criterion) {
    // `cqlite_core::query` (and with it `build_row_from_scan_cached`) is gated on
    // `state_machine`; without it there is nothing to convert. The target still
    // compiles and runs, reporting no measurements.
    eprintln!(
        "row_build: requires the `state_machine` feature (default-on). \
         Run: cargo bench -p cqlite-core --bench row_build"
    );
}

#[cfg(not(feature = "state_machine"))]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_noop
);

criterion_main!(benches);
