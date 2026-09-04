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
        // NO DECLARED GAP — and the absence is the assertion. `employee.home` is a
        // `frozen<address>` nested inside a `frozen<employee>`; until #3631 both
        // egress formats emitted the inner UDT's RAW BYTES as blob hex where the
        // golden decodes an object (`{"street": "1 Navy Way", …}`), carried here as
        // a FIELD-scoped `e.home` skip for
        // `Divergence::NestedFrozenUdtRendersAsBlobHex`.
        //
        // #3631 made that value decode and the gap RETIRED ITSELF, which is what
        // `Report::stale_skips` is for: with the skip still declared BOTH lanes
        // FAILed — "the two sides AGREE at that path now, so the exclusion
        // suppresses nothing and is holding back recovered coverage". Removing it
        // is the only sound response, and `e.home` is now value-compared in both
        // formats against the Cassandra-written `sstabledump` golden — a third
        // oracle for #3631, independent of the unit and integration coverage.
        skips: &[],
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
    // The inet/time multicell-collection ORDERING fixture (#3790). COMPARED, not
    // excluded: its only non-plain shape is a multicell cell tombstone, which
    // `golden_dump_shapes` says "cannot justify an exclusion".
    Case {
        presence: Presence::Committed,
        keyspace: "test_comparator_order",
        table: "collection_order",
        schema: "issue-3790-comparator-ordering",
        pk: &["id"],
        ck: &[],
        multicell: &[
            ("inet_set", Multicell::Set),
            ("inet_map", Multicell::Map),
            ("time_set", Multicell::Set),
            ("time_map", Multicell::Map),
            ("pair_set", Multicell::Set),
        ],
        // Two MEASURED rendering divergences, neither of them ordering. What each
        // skip costs for ORDER differs, and saying "order is still compared" of
        // both would be false (roborev job 69):
        //   * inet_set: the matcher proves the two sides are the SAME address, so a
        //     reordered set fails — element [0] would pair ::1 against 9.0.0.1.
        //   * pair_set: NestedFrozenValueLeftUndecodedByGolden compares no content,
        //     so THIS LANE would not notice the tuples being reordered with the
        //     count preserved. That order is pinned instead by
        //     cqlite-core/tests/issue_3790_collection_order_cassandra_golden.rs,
        //     which asserts pair_set's cell-path sequence (inet-major, time-minor,
        //     both partitions) directly against the same golden.
        skips: &[
            Skip {
                path: "inet_set",
                formats: BOTH,
                divergence: Divergence::InetIpv6RendersCompressed,
                why: "golden spells IPv6 expanded (getHostAddress), egress compressed; the matcher proves both parse to the SAME address, and element ORDER is still compared",
            },
            Skip {
                path: "inet_map",
                formats: BOTH,
                divergence: Divergence::InetMapKeyIpv6SpellingNotPairableByThisLane,
                why: "inet map keys never pair across the two IPv6 spellings, so the column is NOT COMPARED AT ALL (null/malformed/wrong-COUNT unchecked); its ORDER is pinned by issue_3790_collection_order_cassandra_golden.rs",
            },
            Skip {
                path: "pair_set",
                formats: BOTH,
                divergence: Divergence::NestedFrozenValueLeftUndecodedByGolden,
                why: "golden leaves the frozen tuple<inet, time> as colon-joined text while the CLI decodes it; only the SHAPE is checked — so a REORDERING of the tuples is NOT detected here either, and pair_set's order is pinned by issue_3790_collection_order_cassandra_golden.rs instead",
            },
        ],
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
        // TWO declared gaps, JSON-lane-only, and NEITHER is an egress defect
        // awaiting a fix (issue #3644 items 2 and 3) — each variant's own docs
        // carry the oracle. `sf`'s non-finite `null` is what
        // `DoubleType.toJSONString:114-123` returns, and the golden's quoted
        // `"Infinity"`/`"NaN"` are the cell-PATH `getString` artifact
        // (`JsonTransformer.java:452`), not the egress oracle; `sd`'s remaining gap
        // is this COMPARATOR's f64 parse, the egress having been fixed to emit the
        // unquoted number `DecimalType.toJSONString:314-317` requires. Both columns
        // are compared IN FULL in the CSV lane, where every cell is text and the
        // three tokens and all 33 digits survive verbatim — a `BOTH` scope dropped
        // the whole column from a format that renders it correctly (finding K1).
        skips: &[
            Skip {
                path: "sf",
                formats: &[Egress::Json],
                divergence: Divergence::NonFiniteFloatRendersAsJsonNull,
                why: "CORRECT BEHAVIOUR, not a defect: cassandra-5.0.8 \
                      DoubleType.toJSONString:114-123 returns the literal `null` for \
                      NaN/Infinity/-Infinity, and CQLite matches it; the golden's quoted \
                      tokens are the cell-PATH getString artifact \
                      (JsonTransformer.java:452), not the egress oracle. The set's \
                      FINITE members are compared",
            },
            Skip {
                path: "sd",
                formats: &[Egress::Json],
                divergence: Divergence::ExactDecimalNotCarriedByThisLanesJsonParse,
                why: "COMPARATOR LIMITATION, not an egress divergence: the CLI emits the \
                      unquoted number DecimalType.toJSONString:314-317 requires, but this \
                      lane's JSON parse holds it as an f64; both sides must still be the \
                      same double, the CSV lane compares every digit, and the egress text \
                      is pinned by tests/issue_3644_json_decimal_unquoted.rs",
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
    // "ONLY" is stated with its reason, because the obvious candidate is NOT this
    // one: `test_basic.simple_table` (test-data/schemas/basic-types.cql) declares
    // `height FLOAT` and is the very table #3777's smoke golden and oracle quote.
    // This lane cannot take it — its committed dump carries `ttl` liveness keys,
    // which `golden_dump_shapes::unsupported_shapes` refuses as
    // `Unsupported::Ttl`, so the golden reader would reject the case rather than
    // compare it. Measured, not assumed: three `ttl` occurrences in that table's
    // `nb-1-big-Data.db.jsonl`. If TTL ever becomes comparable here, that table is
    // a second FLOAT case and this note is what says so.
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
        // COMPARED IN FULL: `id`, `f_set_tuple_udt`, and — since issue #3726 gave
        // `Canon` a container representation — the three frozen container-keyed maps
        // `f_map_tuple_udt`, `f_map_set_udt` and `f_map_tuple_list_udt`. Everything
        // else is excluded, in TWO distinct classes.
        //
        // Stated as the SURVIVING SET rather than as a count of the excluded: a count
        // here drifted once already (roborev job 308 caught "Eight" after a ninth skip
        // was added), and it drifted even though it sits in the same file as the `Skip`
        // list that invalidates it — co-location is not enough when the edit that
        // changes the number is not the edit that reads it.
        //
        // AND THE SURVIVING SET DRIFTED TOO, one issue later: #3726 removed three skips
        // and this sentence still read "every column except `id` and
        // `f_set_tuple_udt`" until roborev job 14 caught it. So the lesson generalises
        // past the count — ANY prose census of this list decays, because the edit that
        // changes it is the `Skip` array below and nothing makes you re-read this. The
        // authoritative figure is emitted by the census line itself ("N cells compared,
        // M of them containers"), which is DERIVED and cannot drift; this prose exists
        // only to say WHICH columns and WHY, and must be re-read whenever a `Skip` is
        // added or removed.
        //
        // CLASS 1 — the golden leaves the nested frozen element UNDECODED (raw
        // bytes as hex for a collection, colon-joined text for a tuple) while the
        // CLI decodes it. A value disagreement, in the direction OPPOSITE to
        // `NestedFrozenUdtRendersAsBlobHex`.
        //
        // CLASS 2 — WAS its own class and is now CLASS 1, which is the point. It
        // covered four columns as a LANE limitation (golden left a MULTICELL map's
        // container key as getString text, CLI rendered raw bytes as `0x`, NEITHER
        // decoded). #3726 closed three (the FROZEN maps, compared in full); #3612
        // then taught the CLI to decode the cell path, so the egress half is gone
        // and only the GOLDEN's non-decode remains — Class 1's divergence exactly.
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
            // THE ONE MULTICELL CONTAINER-KEYED MAP. The other three
            // (`f_map_tuple_udt`, `f_map_set_udt`, `f_map_tuple_list_udt`) were
            // skipped here under `ContainerMapKeyNotPairableByThisLane` until issue
            // #3726 gave `Canon` a container representation; they are now compared in
            // full, in both formats, and their skips are GONE — which is the
            // self-retiring link that scaffold was accepted for.
            //
            // This one cannot follow them, and the reason is MEASURED rather than
            // structural: it is the only NON-frozen map of the four, so its entries
            // are separate cells whose key is the cell PATH, which
            // `cassandra-5.0.8 JsonTransformer.serializeCell` writes with
            // `writeString(getString(...))`. The golden therefore carries
            // `TupleType.getString`'s colon-joined text (`"charlie\:3:8"`). THE EGRESS
            // CHANGED UNDER THIS BRANCH: until #3612 (`8c503f7cf`) the CLI rendered the
            // key's raw bytes as a blob literal, so NEITHER side decoded it; that commit
            // decodes a multicell composite cell path STRUCTURALLY, so the CLI now emits
            // `[{label: charlie, rank: 3}, 8]` (measured after rebasing). `stale_skips`
            // FAILED the lane over the now-false declaration; what remains is the
            // GOLDEN's non-decode, which the five siblings above already declare.
            Skip {
                path: "m_tuple_udt",
                formats: BOTH,
                divergence: Divergence::NestedFrozenValueLeftUndecodedByGolden,
                why: "the ONE multicell map here, and since #3612 landed it is the SAME divergence its five siblings above already declare: the golden leaves the tuple key as sstabledump's colon-joined getString cell-path text while the CLI now DECODES it into a structure. Only the KEY'S CONTENT is uncompared; the entry count, the {key,value} shape and every entry VALUE (paired in emitted order) ARE compared",
            },
        ],
    },
];
