//! WHICH TABLES the AD2 lane compares (issue #1491): the `CASES` declaration and
//! nothing else.
//!
//! Split out of `issue_1491_json_csv_golden_parity.rs` under the campsite rule
//! (CLAUDE.md, epic #1135), which had passed the ~1500-line test-file target. The
//! boundary is a responsibility one: this file is the DATA — which fixture, which
//! committed schema, which key columns, which declared gaps — while the lane file
//! is the machinery that resolves, exports and compares. Its sibling
//! `issue_1491_coverage_census.rs` asks which committed fixtures this set is
//! accountable for, and reads the same declaration.
//!
//! Declared as a child module of the lane, so `Case`, `Presence`, `Skip`, `BOTH`
//! and the golden-support types are the lane's own — there is no second copy of
//! any of them to drift.

use super::{Case, Presence, Skip, BOTH};
use crate::golden::compare::gap::Divergence;
use crate::golden::{Egress, Multicell};

/// Committed fixture tables whose golden is a pure set of live rows, so the
/// physical dump and the CLI's reconciled result set are the same rows.
///
/// Key columns are transcribed from the committed `CREATE TABLE` named by
/// `schema`. A wrong transcription cannot pass: the column names become row keys
/// compared against the CLI's own, and the golden's key arity is asserted against
/// the declared arity per row.
pub(crate) const CASES: &[Case] = &[
    // test-data/schemas/compression-parity.cql — (pk int, ck int, body text).
    // Seven codec variants: the same logical rows through LZ4 / Snappy / Deflate /
    // Zstd / uncompressed / a short final chunk, plus a BLOB payload table.
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "lz4_table",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "snappy_table",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "deflate_table",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "zstd_table",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "uncompressed_table",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "short_final_chunk",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    // `payload BLOB` — the blob `0x…` hex rendering, compared byte-exactly.
    Case {
        presence: Presence::Committed,
        keyspace: "test_comp",
        table: "incompressible_uncompressed_chunk",
        schema: "compression-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    // test-data/schemas/compaction-parity.cql
    Case {
        presence: Presence::Committed,
        keyspace: "test_compactionparity",
        table: "live_no_clustering",
        schema: "compaction-parity",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_compactionparity",
        table: "live_clustering",
        schema: "compaction-parity",
        pk: &["id"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    // test-data/schemas/compaction-parity-udt.cql — frozen UDTs and frozen
    // collections OF UDTs, i.e. the `_type`-discriminator and map-spelling rules.
    Case {
        presence: Presence::Committed,
        keyspace: "test_compactionparityudt",
        table: "udt_frozen_person",
        schema: "compaction-parity-udt",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_compactionparityudt",
        table: "udt_collections",
        schema: "compaction-parity-udt",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_compactionparityudt",
        table: "udt_null_inner",
        schema: "compaction-parity-udt",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_compactionparityudt",
        table: "udt_nested",
        schema: "compaction-parity-udt",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        // MEASURED DIVERGENCE, not a normalization: `employee.home` is a
        // `frozen<address>` nested inside a `frozen<employee>`. The golden decodes
        // it (`{"street": "1 Navy Way", …}`); both CLI egress formats emit the
        // inner UDT's RAW BYTES as blob hex
        // (`0x0000000a31204e617679205761790000000941726c696e67746f6e…`).
        //
        // The exclusion is FIELD-scoped (`e.home`, not `e`) so the sibling fields
        // `e.name` and `e.level` are still value-compared. Excluding the whole
        // column left this case comparing nothing but its primary key while the
        // comment claimed otherwise (review finding F5).
        skips: &[Skip {
            path: "e.home",
            formats: BOTH,
            divergence: Divergence::NestedFrozenUdtRendersAsBlobHex,
            why: "nested frozen UDT renders as blob hex, not a decoded object",
        }],
    },
    // test-data/schemas/signed-collection-parity.cql — NON-frozen and frozen
    // collections of signed numerics: the "path is a JSON string, CLI element is a
    // JSON number" rule, and exact 30-digit decimal text.
    Case {
        presence: Presence::Committed,
        keyspace: "test_signed_coll",
        table: "signed_int_collections",
        schema: "signed-collection-parity",
        pk: &["id"],
        ck: &[],
        multicell: &[("s", Multicell::Set), ("m", Multicell::Map)],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_signed_coll",
        table: "frozen_int_collections",
        schema: "signed-collection-parity",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_signed_coll",
        table: "signed_width_collections",
        schema: "signed-collection-parity",
        pk: &["id"],
        ck: &[],
        multicell: &[
            ("sb", Multicell::Set),
            ("ss", Multicell::Set),
            ("st", Multicell::Set),
        ],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_signed_coll",
        table: "signed_special_collections",
        schema: "signed-collection-parity",
        pk: &["id"],
        ck: &[],
        multicell: &[("sd", Multicell::Set), ("sf", Multicell::Set)],
        // MEASURED DIVERGENCE, and a JSON-ONLY one: `sf` is a `set<double>`
        // containing `Infinity`, `-Infinity` and `NaN`. The golden carries all
        // three by name (`["-Infinity",…,"Infinity","NaN"]`); JSON has no literal
        // for them, so the JSON egress emits `null` and the value is lost. The CSV
        // egress renders every cell as text and carries the same three tokens
        // verbatim (`{-Infinity, -1.5, -0e0, 0e0, 2.5, Infinity, NaN}`, which the
        // decimal canonicalization reads as the golden's `-0.0`/`0.0`), so CSV IS
        // compared here — a `BOTH` scope dropped the whole column from a format
        // that renders it correctly (review finding K1).
        //
        // A SECOND measured divergence, and JSON-only for the same reason: `sd`
        // (`set<decimal>`, exact 30-digit text) is compared in the CSV lane, where
        // every cell is text and the 30-digit values match exactly, but in the JSON
        // lane the egress renders a `decimal` as a JSON STRING
        // (`"-999999999999999999999999999999.999"`) where `cassandra-5.0.8`
        // `DecimalType.toJSONString` returns `BigDecimal.toString()` UNQUOTED, i.e.
        // a JSON number. The divergence is a property of the type, not of the
        // position, so it would show on a scalar `decimal` column too; it surfaced
        // here because `sd` is the only `decimal` in any compared case. It only
        // became visible once the kinding relaxation stopped being applied to the
        // CLI side (review finding M1) — while it was symmetric, the CLI's string
        // was read as a number at this stringified position.
        skips: &[
            Skip {
                path: "sf",
                formats: &[Egress::Json],
                divergence: Divergence::NonFiniteFloatRendersAsJsonNull,
                why: "set<double> Infinity/-Infinity/NaN render as JSON null — JSON has \
                      no literal for them; the set's FINITE members are compared",
            },
            Skip {
                path: "sd",
                formats: &[Egress::Json],
                divergence: Divergence::DecimalRendersAsJsonString,
                why: "decimal renders as a JSON string where cassandra-5.0.8 \
                      DecimalType.toJSONString emits an unquoted number; the quoted \
                      NUMBER must still equal the golden's",
            },
        ],
    },
    // test-data/schemas/da-test.cql — BTI (`da`) format, timestamp/uuid/boolean
    // scalars plus non-frozen set/list/map.
    Case {
        presence: Presence::Committed,
        keyspace: "test_da",
        table: "simple_table",
        schema: "da-test",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_da",
        table: "collection_table",
        schema: "da-test",
        pk: &["id"],
        ck: &[],
        multicell: &[
            ("tags", Multicell::Set),
            ("scores", Multicell::List),
            ("properties", Multicell::Map),
        ],
        skips: &[],
    },
    // BTI wide/multi-clustering shapes: many rows per partition, so row pairing
    // and clustering-column rendering are exercised at scale.
    Case {
        presence: Presence::Committed,
        keyspace: "test_da",
        table: "wide_table",
        schema: "wide-table-bti",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_da",
        table: "multiclustering_table",
        schema: "multiclustering-table-bti",
        pk: &["pk"],
        ck: &["bucket", "seq"],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_da",
        table: "wide_multiclustering_small",
        schema: "wide-multiclustering-small-bti",
        pk: &["pk"],
        ck: &["bucket", "seq"],
        multicell: &[],
        skips: &[],
    },
    // test-data/schemas/write-load-parity.cql
    Case {
        presence: Presence::Committed,
        keyspace: "test_writeparity",
        table: "finished_data",
        schema: "write-load-parity",
        pk: &["id"],
        ck: &[],
        multicell: &[],
        skips: &[],
    },
    Case {
        presence: Presence::Committed,
        keyspace: "test_writeparity",
        table: "partition_boundary",
        schema: "write-load-parity",
        pk: &["id"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    // ---------------------------------------------------------------------
    // FETCHED-corpus tier. Each of these tables carries a property NO committed
    // fixture has, so the tier exists to cover what the committed set cannot.
    //
    // The `test_types` tables (test-data/schemas/cql-type-parity.cql) carry the
    // null/empty/absent properties — verified by scanning every committed golden:
    // none of them has a row that omits a regular cell, so without this tier "an
    // absent cell renders as null" and "a cell tombstone renders as null" would be
    // unasserted. `test_timeseries.sensor_data` carries the only `FLOAT` this lane
    // can reach (see its own note below). Stated as a tier rather than as a count,
    // which drifts (roborev job 308).
    // ---------------------------------------------------------------------

    // Row 1 omits `reg` (never written), row 2 carries a CELL TOMBSTONE for it,
    // row 3 writes it as the empty string: absent vs deleted vs empty, the three
    // spellings a formatter can confuse.
    Case {
        presence: Presence::Corpus,
        keyspace: "test_types",
        table: "nb_absent_vs_null_regular",
        schema: "cql-type-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    // `target_text`/`target_blob` cycle through absent / NULL / '' / 0x with live
    // neighbours either side, so a shifted or swallowed value is visible.
    Case {
        presence: Presence::Corpus,
        keyspace: "test_types",
        table: "nb_null_empty_text_blob",
        schema: "cql-type-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    // text/blob at length 0, 1, 127, 128, 255, 256, 16383, 16384 — the
    // length-prefix edges, where a truncating formatter shows up.
    Case {
        presence: Presence::Corpus,
        keyspace: "test_types",
        table: "nb_length_prefix_edges",
        schema: "cql-type-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[],
        skips: &[],
    },
    // An EMPTY multicell collection is stored ABSENT by Cassandra (the dump holds
    // only a complex deletion) while an empty FROZEN one persists as a present
    // empty value. `fl`/`fs`/`fm` therefore pin `[]` and `{}` as PRESENT empty
    // containers, which is the half of the property CQLite gets right.
    Case {
        presence: Presence::Corpus,
        keyspace: "test_types",
        table: "nb_empty_collections",
        schema: "cql-type-parity",
        pk: &["pk"],
        ck: &["ck"],
        multicell: &[
            ("ml", Multicell::List),
            ("ms", Multicell::Set),
            ("mm", Multicell::Map),
        ],
        // MEASURED DIVERGENCE: for the row whose multicell collections were
        // written EMPTY, the golden carries a complex deletion and no cells — i.e.
        // the column is absent, and Cassandra's `SELECT` returns `null` (the DDL
        // comment in cql-type-parity.cql states this, and the same on-disk shape is
        // what `DELETE ml FROM …` writes). Both CQLite egress formats instead
        // render a PRESENT empty container (`[]`, `{}`), which is a different
        // value. Non-empty multicell rendering stays covered by four other cases
        // (test_da.collection_table and the three test_signed_coll tables).
        skips: &[
            Skip {
                path: "ml",
                formats: BOTH,
                divergence: Divergence::AbsentMulticellRendersEmpty,
                why: "empty multicell list renders as [] where Cassandra reads null",
            },
            Skip {
                path: "ms",
                formats: BOTH,
                divergence: Divergence::AbsentMulticellRendersEmpty,
                why: "empty multicell set renders as {} where Cassandra reads null",
            },
            Skip {
                path: "mm",
                formats: BOTH,
                divergence: Divergence::AbsentMulticellRendersEmpty,
                why: "empty multicell map renders as {} where Cassandra reads null",
            },
        ],
    },
    // test-data/schemas/time-series.cql — the ONLY `FLOAT` column this lane can
    // reach (issue #3777). NO committed `*-Data.db` fixture declares a `float`
    // anywhere: measured across every committed schema, so before this case the
    // lane compared no f32 at all and could not see the JSON writer widening an
    // f32 to f64 before serializing — `1.6699999570846558` where sstabledump (and
    // Cassandra `FloatSerializer` -> `Float.toString`) spells `1.67`. That
    // divergence lived in a README note instead of a test for exactly that reason.
    //
    // `temperature`/`humidity` are FLOAT and carry the full 7-9 significant digits
    // an f32 holds (`92.88221`, `-16.172066`, `1.5052613`), so a widened spelling
    // cannot pass as a rounding coincidence; `pressure DOUBLE` sits beside them, so
    // the f64 path is compared in the same rows. Ten live partitions, no tombstone,
    // no TTL, no static column — the golden reader accepts it whole.
    Case {
        presence: Presence::Corpus,
        keyspace: "test_timeseries",
        table: "sensor_data",
        schema: "time-series",
        pk: &["sensor_id"],
        ck: &["timestamp"],
        multicell: &[],
        // MEASURED DIVERGENCE, and a CSV-ONLY one: exactly ONE of this table's 1000
        // `temperature` cells (`sensor_id=bc9e0632-1319-472a-a38e-ff5b54cf7ef8`) is
        // an f32 whose shortest decimal is an EXACT TIE — 36.6015625, where
        // `36.601562` and `36.601563` are equidistant and both round-trip. The
        // golden carries `36.601562` (`Float.toString` breaks a tie to an EVEN last
        // digit); the CSV and table writers render through
        // `ValueFormatter::format_float32`, i.e. Rust's `f32` `Display`, which
        // rounds away from zero and emits `36.601563`. Same f32, different spelling
        // — the gap REQUIRES identical f32 bits, so a genuine value error here is an
        // ordinary diff.
        //
        // The JSON lane is NOT excluded and compares this cell: #3777 made the JSON
        // writer format the f32 as an f32 (through serde_json's own Ryū-family
        // formatter, which breaks ties to even), so it agrees with the oracle. The
        // shared CSV/table formatter in cqlite-core is the remaining half and is its
        // own change; when it is fixed this gap goes stale and FAILS this lane,
        // which is what removes it.
        //
        // The cost is ONE CELL, measured from the census lines rather than assumed:
        // the CSV lane compares 15999 cells against the JSON lane's 16000, because a
        // gap suppresses only the node where its declared divergence is OBSERVED —
        // the other 999 `temperature` cells agree and are compared normally, as are
        // `humidity` (also FLOAT) and `pressure` (DOUBLE) in both lanes.
        skips: &[Skip {
            path: "temperature",
            formats: &[Egress::Csv],
            divergence: Divergence::Float32TieBreakSpellingDiffersFromJava,
            why: "an exact-tie f32 (36.6015625) is spelled 36.601563 by the shared \
                  CSV/table formatter where Float.toString spells the tie-to-even \
                  36.601562 — the SAME f32, only the tie-break digit differs",
        }],
    },
    // test-data/schemas/nested-udt-keys.cql — (id int PRIMARY KEY) plus ten
    // columns nesting the `key_part` UDT inside tuples, sets, lists and maps,
    // both as map KEYS and as map VALUES (issue #3500). Comparable: the golden
    // carries no shape `unsupported_shapes` refuses — no partition or row
    // deletion, no TTL, no static block, and only `row` elements. Its
    // `marked_deleted` markers are all CELL-level collection tombstones on the
    // six non-frozen columns, which reconcile to a value this lane compares
    // (see `coverage_census::NOT_COMPARABLE`'s note on cell deletions), so an
    // exclusion here would be coverage loss rather than a reason.
    //
    // `multicell` names exactly the six NON-frozen collections from the DDL;
    // the four `f_*` columns are `frozen<…>` and therefore single-cell.
    Case {
        presence: Presence::Committed,
        keyspace: "test_nested_udt_keys",
        table: "nested_udt_keys",
        schema: "nested-udt-keys",
        pk: &["id"],
        ck: &[],
        multicell: &[
            ("s_tuple_udt", Multicell::Set),
            ("s_set_udt", Multicell::Set),
            ("m_tuple_udt", Multicell::Map),
            ("s_list_udt", Multicell::Set),
            ("s_map_udt_key", Multicell::Set),
            ("s_map_udt_val", Multicell::Set),
        ],
        // EVERY column except `id` and `f_set_tuple_udt` is excluded, in TWO
        // distinct classes. Stated as the SURVIVING SET rather than as a count of
        // the excluded: a count here drifted once already (roborev job 308 caught
        // "Eight" after a ninth skip was added), and it drifted even though it sits
        // in the same file as the `Skip` list that invalidates it — co-location is
        // not enough when the edit that changes the number is not the edit that
        // reads it. The surviving set is also what a reader needs: `f_set_tuple_udt`
        // is a genuinely nested frozen column that IS compared, which is why this is
        // a CASES entry rather than a NOT_COMPARABLE one. The authoritative count is
        // emitted by the census line itself ("N cells compared, M of them
        // containers") and needs no prose duplicate.
        //
        // CLASS 1 — the golden leaves the nested frozen element UNDECODED (raw
        // bytes as hex for a collection, colon-joined text for a tuple) while the
        // CLI decodes it. A value disagreement, in the direction OPPOSITE to
        // `NestedFrozenUdtRendersAsBlobHex`.
        //
        // CLASS 2 — a LANE LIMITATION, not a disagreement: the declared map KEY is
        // a container and this lane has no pairing rule for one, so the two sides
        // are never compared. Tracked for real support in #3726; when that lands
        // these four skips go stale and FAIL, which is what removes them.
        skips: &[
            Skip {
                path: "s_tuple_udt",
                formats: BOTH,
                divergence: Divergence::NestedFrozenValueLeftUndecodedByGolden,
                why: "golden leaves the frozen tuple element as sstabledump's colon-joined text while the CLI decodes it; only the SHAPE is checked — the element CONTENT is NOT compared",
            },
            Skip {
                path: "s_set_udt",
                formats: BOTH,
                divergence: Divergence::NestedFrozenValueLeftUndecodedByGolden,
                why: "golden leaves the frozen inner set as raw serialized hex while the CLI decodes it; only the SHAPE is checked — the element CONTENT is NOT compared",
            },
            Skip {
                path: "s_list_udt",
                formats: BOTH,
                divergence: Divergence::NestedFrozenValueLeftUndecodedByGolden,
                why: "golden leaves the frozen inner list as raw serialized hex while the CLI decodes it; only the SHAPE is checked — the element CONTENT is NOT compared",
            },
            Skip {
                path: "s_map_udt_key",
                formats: BOTH,
                divergence: Divergence::NestedFrozenValueLeftUndecodedByGolden,
                why: "golden leaves the frozen inner map as raw serialized hex while the CLI decodes it; only the SHAPE is checked — the element CONTENT is NOT compared",
            },
            Skip {
                path: "s_map_udt_val",
                formats: BOTH,
                divergence: Divergence::NestedFrozenValueLeftUndecodedByGolden,
                why: "golden leaves the frozen inner map (UDT as VALUE) as raw serialized hex while the CLI decodes it; only the SHAPE is checked — the element CONTENT is NOT compared",
            },
            // THE FOUR CONTAINER-KEYED MAPS — a LANE limitation, not a value
            // disagreement. `compare_map` pairs entries by canonical SCALAR key form
            // and refuses a container key outright, so these columns are not compared
            // AT ALL. The skip is whole-column and therefore OVER-SKIPS: it also
            // suppresses a null, a malformed {key,value} array, a wrong entry count
            // and a wrong tuple arity here. That cost is accepted and documented
            // rather than bounded — three review rounds (roborev 302/305/306) showed
            // that bounding it means reimplementing `compare_map`'s own feature list,
            // because a Skip is path-scoped to a column and cannot express "compare
            // everything except the keys". Real support needs a container
            // representation in `Canon` (scalar-only today); tracked in #3726, and
            // these four skips go stale and FAIL the lane when it lands.
            Skip {
                path: "m_tuple_udt",
                formats: BOTH,
                divergence: Divergence::ContainerMapKeyNotPairableByThisLane,
                why: "map key is tuple<key_part, int> — this lane pairs map keys by canonical scalar form only, so the column is NOT COMPARED AT ALL: a null, a malformed {key,value} array, a wrong entry COUNT and a wrong tuple ARITY are all UNCHECKED here (#3726)",
            },
            Skip {
                path: "f_map_tuple_udt",
                formats: BOTH,
                divergence: Divergence::ContainerMapKeyNotPairableByThisLane,
                why: "map key is frozen tuple<key_part, int> — column NOT COMPARED AT ALL: a null, a malformed {key,value} array, a wrong entry COUNT and a wrong tuple ARITY are all UNCHECKED here (#3726)",
            },
            Skip {
                path: "f_map_set_udt",
                formats: BOTH,
                divergence: Divergence::ContainerMapKeyNotPairableByThisLane,
                why: "map key is frozen set<key_part> — column NOT COMPARED AT ALL: a null, a malformed {key,value} array and a wrong entry COUNT are all UNCHECKED here (#3726)",
            },
            Skip {
                path: "f_map_tuple_list_udt",
                formats: BOTH,
                divergence: Divergence::ContainerMapKeyNotPairableByThisLane,
                why: "map key is frozen tuple<list<key_part>, int> — column NOT COMPARED AT ALL: a null, a malformed {key,value} array, a wrong entry COUNT and a wrong tuple ARITY are all UNCHECKED here (#3726)",
            },
        ],
    },
];
