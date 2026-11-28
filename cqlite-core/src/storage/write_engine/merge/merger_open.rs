//! `KWayMerger`'s public constructors and the one-shot `merge` driver (split out of
//! `merge/mod.rs`, #4658).

use super::*;

#[cfg(feature = "write-support")]
impl KWayMerger {
    /// Create a new k-way merger from input SSTable paths
    ///
    /// # Arguments
    ///
    /// * `input_paths` - Paths to input SSTable Data.db files (ordered newest to oldest)
    /// * `schema` - Table schema for schema-aware merging
    ///
    /// # Returns
    ///
    /// A new KWayMerger ready to merge the input SSTables.
    ///
    /// # Errors
    ///
    /// Returns an error if any input SSTable cannot be opened.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let input_paths = vec![
    ///     PathBuf::from("data/nb-1-big-Data.db"),
    ///     PathBuf::from("data/nb-2-big-Data.db"),
    /// ];
    /// let merger = KWayMerger::new(input_paths, &schema)?;
    /// ```
    pub fn new(input_paths: Vec<PathBuf>, schema: &TableSchema) -> Result<Self> {
        Self::new_with_gc(input_paths, schema, None, None)
    }

    /// Like [`KWayMerger::new`], but wires a cooperative
    /// [`ScanCancel`](crate::storage::scan_cancel::ScanCancel) into every input
    /// reader's compaction scan (issue #2264). Used by the Flight `do_get` merge
    /// so a client disconnect abandons an in-flight, index-less full-Data.db walk
    /// within milliseconds instead of the ~1–2 min transport backstop.
    pub fn new_cancellable(
        input_paths: Vec<PathBuf>,
        schema: &TableSchema,
        scan_cancel: crate::storage::scan_cancel::ScanCancel,
    ) -> Result<Self> {
        Self::new_with_gc_and_registry_cancellable(
            input_paths,
            schema,
            None,
            None,
            None,
            scan_cancel,
        )
    }

    /// Create a new k-way merger with explicit purge parameters.
    ///
    /// Identical to [`KWayMerger::new`] but threads an explicit gc_grace cutoff
    /// (`gc_before_secs`) and TTL evaluation time (`now_secs`) into the merge.
    /// This is the deterministic entry point used by `compact_sstables` (the
    /// `cqlite compact` CLI command) and the compaction-parity harness (issue
    /// #842): Cassandra's compaction takes the same `gcBefore`, so purge
    /// decisions cannot diverge between the two engines.
    ///
    /// # Arguments
    ///
    /// * `input_paths` - Paths to input SSTable Data.db files (ordered newest to oldest)
    /// * `schema` - Table schema for schema-aware merging
    /// * `gc_before_secs` - gc_grace cutoff (seconds since epoch), or `None` to not purge
    /// * `now_secs` - "now" (seconds since epoch) for TTL expiry, or `None` for engine default
    #[tracing::instrument(name = "merger.new", level = "debug", skip(input_paths, schema, gc_before_secs, now_secs), fields(inputs = input_paths.len()))]
    pub fn new_with_gc(
        input_paths: Vec<PathBuf>,
        schema: &TableSchema,
        gc_before_secs: Option<i64>,
        now_secs: Option<i64>,
    ) -> Result<Self> {
        Self::new_with_gc_and_registry(input_paths, schema, gc_before_secs, now_secs, None)
    }

    /// Like [`KWayMerger::new_with_gc`], but threads an authoritative
    /// [`UdtRegistry`](crate::schema::UdtRegistry) onto every input SSTable reader
    /// so the compaction read path can decode a top-level `frozen<UDT>` cell
    /// structurally (issue #1234). The one-shot `compact_sstables_with_registry`
    /// and the WriteEngine background compaction pass their configured registry
    /// here; the registry-free `new`/`new_with_gc` paths pass `None`.
    #[tracing::instrument(name = "merger.new_registry", level = "debug", skip(input_paths, schema, gc_before_secs, now_secs, udt_registry), fields(inputs = input_paths.len()))]
    pub fn new_with_gc_and_registry(
        input_paths: Vec<PathBuf>,
        schema: &TableSchema,
        gc_before_secs: Option<i64>,
        now_secs: Option<i64>,
        udt_registry: Option<crate::schema::UdtRegistry>,
    ) -> Result<Self> {
        Self::new_with_gc_and_registry_cancellable(
            input_paths,
            schema,
            gc_before_secs,
            now_secs,
            udt_registry,
            crate::storage::scan_cancel::ScanCancel::default(),
        )
    }

    // `with_purge_safe`, `with_now_secs`, `with_gc_before_secs` and
    // `with_max_purgeable_timestamp` (issue #4193) live in the generic
    // `impl<S: TraceSink> KWayMerger<S>` block below so they stay chainable
    // AFTER `with_trace_sink`/`build_single_partition_merger_with_trace` has
    // already produced a `KWayMerger<S>` for a non-`NoTrace` `S` — the explain
    // surface configures a traced point-read merger as a full compaction
    // exactly the way the untraced compaction path configures its own.

    /// Replace the zero-sized default sink with a caller-owned trace sink.
    ///
    /// All merge state is moved without retaining input paths or adding a
    /// trace field to [`MergeEntry`]. Existing constructors therefore remain
    /// `KWayMerger<NoTrace>`, while explain-style callers opt into a traced
    /// merger explicitly.
    #[must_use]
    pub fn with_trace_sink<T: TraceSink>(self, sink: T) -> KWayMerger<T> {
        let KWayMerger {
            runs,
            heap,
            current_partition,
            schema,
            schema_arc,
            gc_before_secs,
            now_secs,
            purge_safe,
            max_purgeable_timestamp,
            _egress_slot,
            trace: _,
        } = self;
        KWayMerger {
            runs,
            heap,
            current_partition,
            schema,
            schema_arc,
            gc_before_secs,
            now_secs,
            purge_safe,
            max_purgeable_timestamp,
            _egress_slot,
            trace: sink,
        }
    }

    // `with_egress_slot` (issue #2765) lives in `egress_budget.rs` (its own
    // `impl KWayMerger` block) to keep this over-threshold file from growing
    // (#1116 campsite rule).

    /// Perform a full merge to the output writer
    ///
    /// This is a convenience method that repeatedly calls `step()` until
    /// the merge is complete, writing each partition to the output writer.
    ///
    /// # Arguments
    ///
    /// * `output_writer` - SSTableWriter to write merged output
    ///
    /// # Returns
    ///
    /// Statistics about the merge operation.
    ///
    /// # Errors
    ///
    /// Returns an error if reading or writing fails.
    pub fn merge(
        mut self,
        output_writer: &mut crate::storage::sstable::writer::SSTableWriter,
    ) -> Result<MergeStats> {
        let start_time = Instant::now();
        let mut stats = MergeStats {
            input_files: self.runs.len(),
            output_partitions: 0,
            output_rows: 0,
            bytes_written: 0,
            elapsed: Duration::from_secs(0), // Will be updated at the end
            // The merger only sees the (already drop-filtered) inputs; the caller
            // that computed the drop-set records it (issue #1388).
            dropped_whole: Vec::new(),
        };

        // Stage 5c-iv part 2 (#1668): feed cluster groups DIRECTLY to the
        // incremental writer entry point (part 1) instead of assembling a
        // whole `Vec<Mutation>` and calling `write_partition` once. The
        // (bounded, partition-size-independent) `clustering_key: None`
        // prefix — a static-row carrier and/or range/partition-tombstone
        // carriers — is ALWAYS emitted before any `Some(ck)` row (proven in
        // `streaming.rs`'s `static_row_carrier_always_sorts_first_
        // regardless_of_partition_width` test), so it is buffered just long
        // enough to resolve the partition tombstone / range-tombstones /
        // static ops that `begin_partition_incremental`/`feed_static_row`
        // need UPFRONT. Every subsequent `Some(ck)` row streams straight
        // through `feed_row` with NO `Vec<Mutation>` accumulation.
        //
        // TWO DISTINCT schemas are in play, and mixing them up silently
        // corrupts the output (found via the real #1019 2-generation
        // dropped-column fixture, which no single-run synthetic unit test
        // exercised): `self.schema` is this merger's DECODE-time schema
        // (e.g. `compact_sstables_with_registry`'s `effective_schema`, which
        // still declares a fully-purged dropped column so its cells can be
        // decoded and purge-evaluated) — used ONLY for
        // `merge_entry_to_mutation`. `output_writer`'s OWN schema is the
        // ENCODE-time `write_schema` — the header/column layout actually
        // committed to Data.db (with fully-purged dropped columns already
        // stripped) — required by every `feed_row`/`feed_static_row`/
        // `finish` call, exactly as `write_partition` always used its own
        // `&self.schema` internally rather than a caller-decoded schema.
        let decode_schema = self.schema.clone();
        let write_schema = output_writer.schema().clone();
        let schema_has_static = write_schema.columns.iter().any(|c| c.is_static);
        let mut stream = StreamingMerger::new(&mut self);

        // Outer loop: one iteration per PARTITION, with all partition-scoped
        // state (including `session`, which borrows both `output_writer` and
        // `range_tombstones`) declared FRESH each iteration. This is
        // required, not stylistic: a `session` variable declared ONCE
        // outside this loop and reassigned across partitions would force
        // the borrow checker to unify ITS lifetime parameters across EVERY
        // partition, making `range_tombstones`/`output_writer` look
        // borrowed for the REST OF THE FUNCTION instead of just the current
        // partition. Re-declaring per outer iteration gives each
        // partition's borrows their own independent, iteration-scoped
        // lifetime.
        'partitions: loop {
            let mut partition_tombstone: Option<PartitionTombstone> = None;
            let mut range_tombstones: Vec<RangeTombstone> = Vec::new();
            let mut static_tracker =
                crate::storage::sstable::writer::data_writer::StaticOpsTracker::new();
            let mut static_first_ts: i64 = 0;
            let mut saw_carrier_or_static = false;
            let mut row_count: u64 = 0;
            // Issue #1383 regression fix (crit2-4), issue #1668: buffer this
            // partition's surviving `Some(ck)` row mutations rather than
            // streaming each straight into the writer session as it arrives.
            //
            // The streaming writer session (`begin_partition_incremental`)
            // needs the partition's COMPLETE range-tombstone set UPFRONT: it
            // sorts the range bounds into on-disk markers and interleaves them
            // with rows in clustering order as each row is fed (and uses them
            // to compute each row's `shadow_floor`). But `StreamingMerger`
            // only surfaces a range tombstone's coalesced marker once its
            // CLOSE bound is parsed — which, for a range whose covered rows
            // live in a different (e.g. newer) generation, is strictly AFTER
            // those rows have already streamed past (issue #1668 stage 5d's
            // "range tombstones can arrive AFTER the rows they cover"
            // finding). Opening the session on the first `Some(ck)` row
            // therefore fixed an EMPTY range-tombstone set, then fed every
            // late marker mutation through `feed_row` — which silently dropped
            // it (a range-only mutation has no row content) — so a synthesized
            // boundary vanished from the compacted output entirely.
            //
            // Buffering the rows until PartitionEnd lets us open the session
            // with the FULL, final range-tombstone set, so `feed_row` shadows
            // every covered row correctly AND interleaves the markers in
            // clustering order. A genuinely bounded-memory streaming WRITE for
            // range-tombstone partitions would need the out-of-scope two-pass
            // reader that surfaces a range's OPEN bound early; this buffer is
            // whole-partition-width for such partitions (the reader already
            // materializes the decompressed section anyway — see the
            // streaming dhat test's module doc), while `StreamingMerger`'s
            // own reconciliation stays row-streamed (the memory bound the
            // dhat proof actually exercises).
            let mut buffered_rows: Vec<crate::storage::write_engine::mutation::Mutation> =
                Vec::new();
            let mut partition_stats = crate::storage::sstable::writer::StatisticsMetadata::new();

            loop {
                match stream.step_streaming()? {
                    StreamingStep::ClusterGroup { key: _, row } => {
                        // Skip truly-empty phantom entries (#886/#899
                        // branch-review) — mirrors the original
                        // `.filter(|entry| !entry.is_metadata_only_no_op())`.
                        if row.is_metadata_only_no_op() {
                            continue;
                        }
                        let mutation = Self::merge_entry_to_mutation(*row, &decode_schema)?;

                        // Classify the (None-keyed) carriers and the static
                        // row; everything else is a real clustering row that
                        // is BUFFERED (see `buffered_rows`' doc) — range
                        // tombstones can still arrive after some rows have
                        // already been buffered, which is exactly why the
                        // whole set must be resolved before the session opens.
                        let is_partition_only = mutation.operations.is_empty()
                            && mutation.partition_tombstone.is_some()
                            && mutation.row_tombstone.is_none()
                            && mutation.range_tombstones.is_empty();
                        let is_range_only = mutation.operations.is_empty()
                            && mutation.partition_tombstone.is_none()
                            && mutation.row_tombstone.is_none()
                            && !mutation.range_tombstones.is_empty();

                        if is_partition_only {
                            // The tombstone MARKER itself is never row-shadowed
                            // — it IS the deletion — so it always contributes
                            // to persisted stats (issue #4246). `is_partition_only`
                            // guarantees `range_tombstones` is empty, so
                            // `fold_marker_stats` folding both fields is exactly
                            // the partition-tombstone-only fold this needs.
                            crate::storage::sstable::writer::stats_fold::fold_marker_stats(
                                &mut partition_stats,
                                &mutation,
                            );
                            partition_tombstone = mutation.partition_tombstone;
                            saw_carrier_or_static = true;
                            continue;
                        }
                        if is_range_only {
                            // `is_range_only` guarantees `partition_tombstone`
                            // is `None`, so `fold_marker_stats` folding both
                            // fields is exactly the range-tombstones-only fold
                            // this needs.
                            crate::storage::sstable::writer::stats_fold::fold_marker_stats(
                                &mut partition_stats,
                                &mutation,
                            );
                            range_tombstones.extend(mutation.range_tombstones.iter().cloned());
                            saw_carrier_or_static = true;
                            continue;
                        }
                        // A `clustering_key: None` mutation is the resolved
                        // static-row carrier ONLY when the schema actually
                        // declares static columns — Cassandra disallows
                        // static columns on a table with no clustering
                        // columns, so for an UNCLUSTERED table EVERY row
                        // (not just a special carrier) uses
                        // `clustering_key: None` too (mirrors
                        // `write_partition_with_index_blocks`'s own
                        // `schema_has_static` gate: without it, that
                        // table's sole per-partition row would be
                        // misrouted here and silently dropped instead of
                        // written as a real row).
                        if mutation.clustering_key.is_none() && schema_has_static {
                            // The resolved static-row carrier (issue #1668
                            // design verification: at most one, always
                            // first). Counted toward `output_rows` (issue
                            // #1238's original `row_mutations` filter
                            // counts ANY mutation with non-empty
                            // `operations` — a static mutation always has
                            // some — so this mirrors that exactly, not
                            // just the Some(ck) rows).
                            if !saw_carrier_or_static {
                                static_first_ts = mutation.timestamp_micros;
                            }
                            // The STATIC-CARRIER fold (issue #4246): row
                            // CONTENT only, with a `None` shadow boundary.
                            // INVARIANT it protects — a static carrier's own
                            // row deletion (`DeleteRow` op or the #932
                            // `row_tombstone` field) must NOT be folded,
                            // because no production path emits a static-row
                            // deletion to Data.db. The #729 flush pre-seed
                            // baseline re-derives the same exclusion
                            // independently in `fold_one_mutation_baseline`
                            // (#4320 tracks unifying the two).
                            // Canonical explanation — the two fold paths, the
                            // pinned `cassandra-5.0.8` authority, why static-
                            // cell shadowing is a separate out-of-scope
                            // question, and why this is `fold_row_content_stats`
                            // with no `fold_marker_stats` (a marker field on
                            // this mutation would be reachable-but-dead, so
                            // there is nothing here to double-count):
                            // `stats_fold::fold_static_carrier_stats`'s doc
                            // comment.
                            crate::storage::sstable::writer::stats_fold::fold_static_carrier_stats(
                                &mut partition_stats,
                                &mutation,
                            );
                            static_tracker.feed(&mutation, &write_schema, None);
                            saw_carrier_or_static = true;
                            row_count += 1;
                            continue;
                        }

                        // A real clustering row (or an unclustered table's
                        // sole `clustering_key: None` row): buffer it for the
                        // single PartitionEnd write once the full
                        // range-tombstone set is known. Its stats fold is
                        // DEFERRED to PartitionEnd (issue #4246): the complete
                        // range-tombstone set — needed to decide whether this
                        // row survives shadowing — is not yet known here (a
                        // range tombstone can arrive AFTER the rows it
                        // covers, see the module doc above).
                        buffered_rows.push(mutation);
                        row_count += 1;
                    }
                    StreamingStep::PartitionEnd { key } => {
                        // Write the whole partition in ONE session now that
                        // the partition tombstone, the COMPLETE coalesced
                        // range-tombstone set, the static row, and every
                        // surviving clustering row are all known. Skip a
                        // truly empty partition (every entry was
                        // metadata-only-no-op), matching the original
                        // `mutations.is_empty()` skip. A partition with only
                        // carriers/statics but no `Some(ck)` row is still
                        // emittable (#933/#1072).
                        if !buffered_rows.is_empty() || saw_carrier_or_static {
                            let mut session = output_writer.begin_partition_incremental(
                                &key,
                                partition_tombstone.as_ref(),
                                &range_tombstones,
                            )?;
                            if schema_has_static {
                                let merged = std::mem::take(&mut static_tracker).finish();
                                session.feed_static_row(&merged, static_first_ts, &write_schema)?;
                            }
                            // Issue #4246: now that `partition_tombstone` and
                            // `range_tombstones` are FINAL, gate each buffered
                            // row's stats fold on the SAME shadow decision
                            // `feed_row` makes when it calls
                            // `DataWriter::merge_row_group` internally — a row
                            // fully shadow-dropped from Data.db must not lower
                            // persisted `StatisticsMetadata` minima. See
                            // `stats_fold::row_group_survives`'s doc comment
                            // for the scope note on mixed static+row
                            // mutations.
                            let partition_floor =
                                partition_tombstone.as_ref().map(|pt| pt.deletion_time);
                            for mutation in &buffered_rows {
                                // Shared with the emitter's own derivation
                                // (issue #4246 roborev round-8 finding 2,
                                // `data_writer/row_groups.rs`) so the fold
                                // can never shadow-gate differently than
                                // `feed_row` does.
                                let shadow_floor =
                                    crate::storage::sstable::writer::data_writer::resolve_shadow_floor(
                                        partition_floor,
                                        &range_tombstones,
                                        mutation.clustering_key.as_ref(),
                                        &write_schema,
                                    );
                                // Issue #4246 roborev round-3 finding: the
                                // four-step fold sequence (resolve survival,
                                // fold the group's own row-deletion marker
                                // once, then gate content on
                                // `carries_static`/`survives`) is shared with
                                // `WriteEngine::maintenance_step`'s
                                // structurally-identical single-mutation
                                // `PartitionEnd` drain via
                                // `stats_fold::fold_single_mutation_row_group`
                                // — extracted so the two paths can never
                                // silently drift apart, and so this module's
                                // own `#1668` equivalence test exercises the
                                // SAME function both production paths call.
                                // `skip_static_ops = false` inside it matches
                                // EXACTLY what `feed_row` itself passes to
                                // `merge_row_group` a few lines below
                                // (`incremental_partition.rs`'s
                                // `DataWriter::merge_row_group(&[mutation],
                                // schema, false, shadow_floor)`).
                                crate::storage::sstable::writer::stats_fold::fold_single_mutation_row_group(
                                    &mut partition_stats,
                                    mutation,
                                    &write_schema,
                                    schema_has_static,
                                    shadow_floor,
                                );
                                session.feed_row(mutation, &write_schema)?;
                            }
                            let (offset, blocks, emit) = session.finish(&write_schema)?;
                            output_writer.complete_partition_incremental(
                                &key,
                                partition_tombstone.as_ref(),
                                offset,
                                &blocks,
                                emit,
                                &partition_stats,
                            )?;
                            stats.output_partitions += 1;
                            stats.output_rows += row_count;
                        }
                        continue 'partitions;
                    }
                    StreamingStep::Complete => break 'partitions,
                }
            }
        }

        // Issue #1238: report the REAL output Data.db byte count instead of a
        // hardcoded 0. The streaming writer flushes each partition as it is
        // written, so its running position is the authoritative total Data.db
        // size — identical to the `data_size` the caller's later
        // `writer.finish()` reports (only residual scratch, normally empty, is
        // flushed there). This is the writer's own byte accounting, not an
        // estimate or a re-stat of the produced file.
        stats.bytes_written = output_writer.data_bytes_written();

        stats.elapsed = start_time.elapsed();
        Ok(stats)
    }
}
