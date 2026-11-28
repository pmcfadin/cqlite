//! Baseline and purge-bound preparation from the inputs' `Statistics.db` (split out of
//! `merge/mod.rs`, #4658): output encoding baselines, the direct-stream guard, the expiry floor,
//! the overlap-aware max-purgeable bound and the `gcBefore` cutoff.

use super::*;

/// Compute output encoding baselines (min timestamp / local-deletion-time / TTL)
/// from the input SSTables' `Statistics.db` files (two-pass compaction, issue #729).
///
/// The output's delta-encoding baseline must be `<=` every per-partition value in
/// every input, so this returns the minimum across all inputs. Each component is
/// left at its `MAX` sentinel when no input contributes a value — matching the
/// value `SSTableWriter::pre_seed_encoding_baselines` then receives.
///
/// Shared by [`compact_sstables`] and `WriteEngine::start_merge` so the one-shot
/// and policy-driven compaction paths seed baselines identically.
#[cfg(feature = "write-support")]
pub fn compute_baseline_min(input_paths: &[PathBuf]) -> (i64, i32, i32) {
    compute_baseline_min_in(&InputStats::resolve(input_paths))
}

/// [`compute_baseline_min`] over inputs already resolved to generations, so the run reads each
/// `Statistics.db` once through the trusted-metadata accessor (G11, GD9.1).
#[cfg(feature = "write-support")]
pub(crate) fn compute_baseline_min_in(inputs: &InputStats) -> (i64, i32, i32) {
    let mut baseline_min_ts = i64::MAX;
    let mut baseline_min_ldt = i32::MAX;
    let mut baseline_min_ttl = i32::MAX;
    for input in inputs.iter() {
        let stats_path = input.path();
        match input.statistics() {
            Err(u) if u.is_absent() => continue,
            Err(u) => {
                tracing::warn!(
                    "Could not read Statistics.db for {:?} for baseline pre-seeding: {}",
                    stats_path,
                    u
                );
            }
            Ok(sstable_stats) => {
                let ts_stats = &sstable_stats.timestamp_stats;
                baseline_min_ts = baseline_min_ts.min(ts_stats.min_timestamp);
                // Local-deletion-time baseline seeding (#853/#886 Finding 2; sentinel
                // exclusion corrected in #1410). The parser reconstructs
                // `min_deletion_time` as `readUnsignedVInt32() + DELETION_TIME_EPOCH`
                // (EncodingStats.java:289). We normalize to the 32-bit signed bit
                // pattern (`as u32 as i32`) — a far-future LDT in [2^31, 2^32) surfaces
                // as an i64 above i32::MAX and is a valid negative-i32 delta baseline
                // (the exact value the DataWriter delta-encodes against), NOT bad data.
                //
                // EXCLUDE any input that carries NO tombstone from the merged LDT-min,
                // matching `EncodingStats.mergeWith` / `EncodingStats.merge`
                // (EncodingStats.java:113-115, 146): a live-only input must NEVER lower
                // the merged min, or a real tombstone LDT is delta-encoded against a
                // too-low baseline and Data.db diverges (#1410: observed cass=99 vs
                // ours=103, first diff at offset 24, where the row-tombstone LDT delta is
                // emitted). This does NOT touch the writer's on-disk format (a header
                // sentinel change would desync the DataWriter's own LIVE-marker baseline,
                // corrupting live complex-column deletion headers — roborev #1410
                // Finding 1); the decision is made HERE, in the compaction baseline seeder.
                //
                // Two distinct on-disk representations mean "this SSTable has no local
                // deletion time" — and CQLite's own writer serializes a live-only SSTable
                // as a bare `0` (StatisticsMetadata::finalize maps the unset `i32::MAX`
                // min to 0), which is INDISTINGUISHABLE from a genuine tombstone whose
                // real LDT is 0 (an old row tombstone with a sub-second write timestamp)
                // by the `min_deletion_time` VALUE alone. So we use an AUTHORITATIVE
                // "has a tombstone" signal, NOT a heuristic on the value (#28): the STATS
                // `estimatedTombstoneDropTime` histogram, which CQLite's writer populates
                // (via `update_local_deletion_time`) and Cassandra writes for EVERY real
                // tombstone LDT — empty iff the SSTable carries no tombstone. We do NOT
                // exclude by LDT value (roborev #1410 Finding 2): `DELETION_TIME_EPOCH`
                // (2015-09-22) is a perfectly valid EXPLICIT tombstone `localDeletionTime`
                // here, so excluding it by value would drop a real tombstone from the
                // baseline and make the writer reject/mis-encode it. The LIVE marker
                // (`i32::MAX` / `NO_DELETION_TIME`) never reaches the histogram
                // (`update_local_deletion_time` filters it), so an empty histogram already
                // covers every no-deletion representation (`0`, `DELETION_TIME_EPOCH`,
                // `i32::MAX`).
                //
                // CONSERVATIVE on a decode failure (roborev #1410 Finding 3): we decode
                // the histogram DIRECTLY here with `parse_stats_extras` and match on its
                // `Result`, NOT `sstable_stats.tombstone_drop_times` — the latter is an
                // EMPTY vec BOTH for a genuinely live-only SSTable AND for a best-effort
                // extras PARSE FAILURE (corrupt / version-mismatched STATS extras), which
                // must not be conflated. We only treat an input as no-tombstone when the
                // extras decode SUCCEEDS with an empty histogram; on ANY decode error we
                // INCLUDE its LDT (cannot prove live-only → never leave the baseline too
                // high, so the merger's re-emitted tombstones stay >= baseline).
                //   * `Ok` + empty histogram → no tombstone → EXCLUDE.
                //   * `Ok` + non-empty histogram → genuine tombstone (incl. real LDT `0`
                //     or `DELETION_TIME_EPOCH`) → INCLUDE.
                //   * `Err` (unparseable extras) → cannot prove live-only → INCLUDE.
                let ldt_bits = ts_stats.min_deletion_time as u32 as i32;
                let has_tombstone = match input.stats_extras() {
                    Ok(extras) => !extras.tombstone_drop_times.is_empty(),
                    Err(e) => {
                        // Unparseable extras: stay conservative and INCLUDE (do NOT skip
                        // a possibly-real LDT baseline). `true` forces the `.min()` below.
                        tracing::debug!(
                            "STATS-extras decode failed for {:?} during baseline seeding; \
                             conservatively including its LDT baseline: {:?}",
                            stats_path,
                            e
                        );
                        true
                    }
                };
                if has_tombstone {
                    baseline_min_ldt = baseline_min_ldt.min(ldt_bits);
                }
                if let Some(min_ttl) = ts_stats.min_ttl {
                    if min_ttl > 0 && min_ttl < i32::MAX as i64 {
                        baseline_min_ttl = baseline_min_ttl.min(min_ttl as i32);
                    }
                }
            }
        }
    }
    (baseline_min_ts, baseline_min_ldt, baseline_min_ttl)
}

/// Issue #2299 fail-CLOSED guard for the direct-stream compaction gate.
///
/// The direct-stream write path (`ActiveMerge::stream_rows_directly`) is selected
/// from `compute_baseline_min`'s LDT baseline surviving at the `i32::MAX`
/// (`NO_DELETION_TIME`) sentinel — read as "no input carries ANY deletion, so
/// there are no range/row/partition tombstones to interleave." But
/// `compute_baseline_min` fails OPEN: an input whose `Statistics.db` is MISSING or
/// fails the top-level parse is silently skipped and never contributes to the
/// baseline. If such a skipped input actually carries a tombstone, the baseline
/// stays at the live sentinel, the direct path is wrongly selected, and that
/// input's tombstones are dropped from the compacted output — previously-deleted
/// data resurrects (a silent data-loss failure mode).
///
/// This returns `false` when ANY input's `Statistics.db` is missing or
/// unparseable at the top level, so the caller can force the always-correct
/// buffered path independently of the (fail-open) baseline seeder. The deletion
/// signal is authoritative metadata only (`Statistics.db`), never a byte heuristic
/// (#28); when it cannot be read we refuse to prove "no deletions" and fall back.
///
/// (An input that PARSES but whose best-effort STATS-extras histogram is
/// unparseable is already handled conservatively inside `compute_baseline_min`:
/// `has_tombstone = true` forces its LDT into the baseline, lowering it below the
/// sentinel — so that case does not reach this guard.)
#[cfg(feature = "write-support")]
pub fn all_input_stats_readable(input_paths: &[PathBuf]) -> bool {
    all_input_stats_readable_in(&InputStats::resolve(input_paths))
}

/// [`all_input_stats_readable`] over inputs already resolved to generations (G11).
#[cfg(feature = "write-support")]
pub(crate) fn all_input_stats_readable_in(inputs: &InputStats) -> bool {
    inputs.iter().all(|input| input.statistics().is_ok())
}

/// Expiry-aware LOWER bound for the compaction output's `min_local_deletion_time`
/// delta baseline (issue #1537).
///
/// When expiry is active, an EXPIRED expiring cell converts to a tombstone whose
/// `localDeletionTime` is the CREATION time `ldt - ttl` (Cassandra
/// `AbstractCell.purge` → `localDeletionTime() - ttl()`) — STRICTLY BELOW the input's
/// `min_deletion_time` (the EXPIRY instant `ldt`). Without lowering, the input-derived
/// baseline from [`compute_baseline_min`] sits ABOVE that tombstone's LDT and the
/// DataWriter's unsigned LDT-delta underflows (the below-baseline guard rejects it).
///
/// For each input carrying expiring cells (authoritative signal: a present, positive
/// `max_ttl` — `EncodingStats` tracks TTL only for expiring cells),
/// `min_deletion_time - max_ttl` is a PROVABLY-SAFE lower bound for every `ldt - ttl`
/// (each cell has `ldt >= min_deletion_time`, `ttl <= max_ttl`), and `<=` every
/// live/unchanged LDT too — so the delta never underflows and the output is a
/// self-consistent, readable SSTable. Returns the min floor across inputs (wrapped
/// `i32` GC-clock bits), or `None` when no input carries expiring cells (no-op).
///
/// NOTE (byte-parity, deferred to #1538): for MIXED-`ttl` inputs this floor can be
/// conservatively lower than Cassandra's exact per-cell `min(ldt_i - ttl_i)` (a safe,
/// metadata-only conservatism — a lower min never wrongly purges); no active
/// byte-parity golden exists for compaction TTL-expiry output (blocked on #1538).
/// No-heuristics: derived solely from authoritative Statistics.db.
pub(crate) fn compute_expiry_ttl_ldt_floor_in(inputs: &InputStats) -> Option<i32> {
    let mut floor: Option<i32> = None;
    for input in inputs.iter() {
        let sstable_stats = match input.statistics() {
            Ok(s) => s,
            Err(u) if u.is_absent() => continue,
            Err(u) => {
                tracing::warn!(
                    "Could not read Statistics.db for {:?} for expiry-TTL LDT floor: {}",
                    input.path(),
                    u
                );
                continue;
            }
        };
        {
            let ts_stats = &sstable_stats.timestamp_stats;
            // Authoritative "has expiring cells" signal: a present, positive
            // `max_ttl` (EncodingStats tracks TTL only for expiring cells).
            let Some(max_ttl) = ts_stats.max_ttl.filter(|&t| t > 0) else {
                continue;
            };
            // Reinterpret the on-disk `min_deletion_time` bits as UNSIGNED GC-clock
            // seconds (mirrors `compute_baseline_min`) before subtracting the TTL.
            let min_ldt_unsigned = i64::from(ts_stats.min_deletion_time as u32);
            // Clamp the floor at 0 before the `as i32` reinterpret. A well-formed
            // GC-clock creation time (`ldt - ttl`) is always >= 0 — Cassandra never
            // writes a pre-epoch localDeletionTime — so `max_ttl > min_deletion_time`
            // (a negative floor) is impossible for real data. The clamp defends the
            // `as i32` cast against a spurious negative bit pattern should a
            // malformed input ever violate that pre-epoch assumption.
            let input_floor_i64 = (min_ldt_unsigned - max_ttl).max(0);
            // Store back as the wrapped i32 GC-clock bit pattern (the DataWriter's
            // delta baseline representation).
            let input_floor = input_floor_i64 as i32;
            floor = Some(match floor {
                Some(cur) => cur.min(input_floor),
                None => input_floor,
            });
        }
    }
    floor
}

/// Compute the overlap-aware max-purgeable timestamp (#935, parity with
/// Cassandra `CompactionController.maxPurgeableTimestamp`) for a PARTIAL
/// compaction from the SSTables NOT included in it.
///
/// `outside_paths` is the set of Data.db paths for the table's SSTables that are
/// NOT part of this compaction but overlap it (conservatively, every other
/// SSTable for the table). The returned value is the MINIMUM write timestamp
/// (`markedForDeleteAt`, micros) across those files — read from each one's
/// `Statistics.db` minimum-timestamp bound (`EncodingStats.minTimestamp`, the same
/// value Cassandra exposes as `SSTableReader.getMinTimestamp()`).
///
/// A tombstone whose own deletion timestamp is STRICTLY LESS THAN this value
/// provably predates all data living outside the compaction set, so purging it can
/// never resurrect shadowed data even in a partial compaction. Thread the result
/// into the merger via [`KWayMerger::with_max_purgeable_timestamp`].
///
/// Returns `None` when `outside_paths` is empty (no overlap — the caller should
/// treat the compaction as full and use `purge_safe`) or when NONE of the outside
/// `Statistics.db` files could be read/parsed (cannot prove safety → stay
/// conservative and do not purge). A missing/unreadable file for one of several
/// outside SSTables is treated conservatively: it cannot raise the bound, so an
/// unreadable outside file leaves the bound unknown and DISABLES overlap-aware
/// purging rather than risk resurrecting its data.
#[cfg(feature = "write-support")]
pub fn compute_max_purgeable_timestamp(outside_paths: &[PathBuf]) -> Option<i64> {
    if outside_paths.is_empty() {
        return None;
    }

    compute_max_purgeable_timestamp_in(&InputStats::resolve(outside_paths))
}

/// [`compute_max_purgeable_timestamp`] over outside SSTables already resolved to generations (G11).
#[cfg(feature = "write-support")]
pub(crate) fn compute_max_purgeable_timestamp_in(outside: &InputStats) -> Option<i64> {
    outside.iter().next()?;
    let mut min_ts = i64::MAX;
    for input in outside.iter() {
        // Any outside SSTable we cannot read/parse leaves the bound UNKNOWN: we
        // can no longer prove a tombstone predates its data, so disable
        // overlap-aware purging entirely (conservative — never resurrect data).
        match input.statistics() {
            Ok(sstable_stats) => {
                min_ts = min_ts.min(sstable_stats.timestamp_stats.min_timestamp);
            }
            Err(u) => {
                if !u.is_absent() {
                    tracing::warn!(
                        "Could not read Statistics.db for {:?} for max-purgeable bound: {}",
                        input.path(),
                        u
                    );
                }
                return None;
            }
        }
    }

    Some(min_ts)
}

/// Compact an explicit set of input SSTables into a single output SSTable.
///
/// Unlike [`WriteEngine::maintenance_step`](super::WriteEngine::maintenance_step),
/// this is a one-shot, policy-free compaction over exactly `input_paths`, writing
/// the merged result to `output_dir` (the writer appends `keyspace/table/`). It is
/// the engine entry point behind the `cqlite compact` CLI command and the
/// compaction-parity harness (issue #842).
///
/// `input_paths` must be ordered newest-to-oldest: index 0 is the newest run, which
/// wins last-write-wins ties at equal timestamp/liveness.
///
/// `gc_before_secs` / `now_secs` are threaded into the merger for deterministic,
/// Cassandra-matching purge decisions. gc_grace tombstone purging IS applied
/// (issue #845), but ONLY when the compaction is overlap-safe — see
/// `KWayMerger::with_purge_safe` (#921 finding 1). TTL expiry (#848) remains
/// carried-but-not-yet-applied.
/// Compute the gc_grace `gcBefore` cutoff (GC-clock seconds) for a compaction.
///
/// Cassandra purges a tombstone during compaction when its on-disk
/// `localDeletionTime` is strictly less than `gcBefore = now - gc_grace_seconds`
/// (parity `8d47ebb2`). CQLite reads `gc_grace_seconds` from the table schema
/// (the `comments` option map — the same surface `cql_parser` uses), since it
/// is a table parameter not recorded inside SSTable files.
///
/// When the table declares NO `gc_grace_seconds` (#921 finding 3), CQLite falls
/// back to Cassandra's table DEFAULT of `864000` seconds (10 days) — CQL tables
/// commonly omit the option and rely on that default, so disabling purging on
/// absence diverged from Cassandra. A value of exactly `0` is valid (immediate
/// grace, `TableParams` allows it) and yields `gcBefore == now`.
///
/// Returns `None` ONLY when the declared value is INVALID — unparseable or
/// NEGATIVE — which DISABLES purging (a strict no-op): garbage metadata must
/// never cause data to be dropped.
#[cfg(feature = "write-support")]
pub fn compute_gc_before(schema: &TableSchema, now_secs: i64) -> Option<i64> {
    /// Cassandra's `TableParams.DEFAULT_GC_GRACE_SECONDS` (10 days).
    const DEFAULT_GC_GRACE_SECONDS: i64 = 864_000;

    match schema.comments.get("gc_grace_seconds") {
        // Absent → Cassandra default (10 days).
        None => Some(now_secs - DEFAULT_GC_GRACE_SECONDS),
        // Present → parse. Reject unparseable or negative values conservatively
        // (return None = no purge); 0 is valid (immediate grace).
        Some(s) => match s.trim().parse::<i64>() {
            Ok(gc_grace_seconds) if gc_grace_seconds >= 0 => Some(now_secs - gc_grace_seconds),
            _ => None,
        },
    }
}
