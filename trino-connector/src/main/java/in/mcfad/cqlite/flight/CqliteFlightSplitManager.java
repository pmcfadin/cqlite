package in.mcfad.cqlite.flight;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import in.mcfad.cqlite.flight.PrimaryKeyExtractor.KeyColumn;

import in.mcfad.cqlite.flight.sidecar.HostAddresses;
import in.mcfad.cqlite.flight.sidecar.SidecarClient;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.ReplicaInfo;
import in.mcfad.cqlite.flight.sidecar.SidecarModels.TokenRangeReplicasResponse;

/**
 * Produces one split per token range, each pinned to a single replica so a row
 * (which lives on RF replicas) is read exactly once across the cluster.
 */
public class CqliteFlightSplitManager implements ConnectorSplitManager {
    private static final System.Logger LOG =
            System.getLogger(CqliteFlightSplitManager.class.getName());

    private final CqliteFlightConfig config;
    private final SidecarClient sidecar;
    private final SnapshotManager snapshots;

    public CqliteFlightSplitManager(CqliteFlightConfig config, SidecarClient sidecar, SnapshotManager snapshots) {
        this.config = config;
        this.sidecar = sidecar;
        this.snapshots = snapshots;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        CqliteFlightTableHandle handle = (CqliteFlightTableHandle) table;
        TokenRangeReplicasResponse replicas = sidecar.tokenRangeReplicas(handle.keyspace());
        // Ring-coverage guard (issue #2237): during Cassandra topology transitions
        // (bootstrap/decommission) the Sidecar's read-replica ranges can transiently
        // OVERLAP (→ duplicate rows) or GAP (→ missing rows), both silent. Verify the
        // returned ranges tile the token ring exactly once BEFORE building splits and
        // fail closed with an actionable error otherwise.
        validateRingCoverage(replicas.readReplicas());
        // Read-mode wiring (issues #2105, #2227): in snapshot mode create (once per query)
        // a Sidecar snapshot on EVERY distinct replica host the scan's splits will read —
        // a snapshot PUT is instance-local, so a snapshot made only on the configured
        // Sidecar's node leaves splits on other hosts reading a non-existent directory
        // (NotFound). We stamp the same snapshot name into every split's ticket so the whole
        // scan reads one immutable file set per host; in live mode this is Optional.empty().
        // Fails closed — a create error on a PRIMARY host propagates (naming host + snapshot)
        // and the query fails; that host must have the snapshot, no failover is possible without it.
        Set<String> primaryHosts =
                distinctReplicaHosts(replicas, config.localDatacenter(), config.subSplitsPerRange());
        // Resolve the reuse window ONCE and thread it into availableHosts below, so the ticket name
        // and the per-fallback-host availability creation operate on the EXACT same window even if
        // the freshness window would otherwise roll between the two calls (issue #2356 roborev,
        // double-resolve race).
        Optional<SnapshotManager.Window> window =
                snapshots.resolveSnapshot(handle.keyspace(), handle.table(), primaryHosts);
        Optional<String> snapshot = window.map(SnapshotManager.Window::name);
        // Availability failover (issue #2241): a fallback host is only usable if IT ALSO has the
        // snapshot. Roborev on #2241: restricting to `primaryHosts` here would only ever admit a
        // fallback that happens to be some OTHER range's primary — a fallback-only host (never a
        // primary anywhere) would be dropped even when its own snapshot creation would have
        // succeeded, so a primary outage could still fail the scan despite another live replica
        // owning the range. Instead, best-effort-create the snapshot on the FULL replica-owner
        // union (every host in every range's replicaHosts()) and keep only the hosts that
        // succeeded. Best-effort here is safe: every host in `primaryHosts` is already required
        // above (fail-closed); only the EXTRA fallback-only hosts can be silently excluded, and a
        // split simply won't list an excluded host as a fallback — never a silent partial result.
        Set<String> availableHosts = window.isPresent()
                ? snapshots.availableHosts(window.get(), handle.keyspace(), handle.table(),
                        allReplicaHosts(replicas, config.localDatacenter()))
                : Set.of();
        List<CqliteFlightSplit> ranges = buildSplits(
                handle, replicas, config.localDatacenter(), config.flightPort(), snapshot, availableHosts,
                config.subSplitsPerRange());

        // Plan-time split pruning (issue #2679): when the pushed-down constraint FULLY binds
        // the partition key (equality on every PK column, or an IN over full keys), compute the
        // Murmur3 token(s) and keep only the covering token range(s). Anything less — or any
        // doubt (unknown partitioner, un-serializable value, exception) — leaves the full
        // fan-out unchanged. Split elimination is load-bearing for correctness (a mis-pruned
        // split silently drops rows), so this is fail-safe by construction. Never applied to an
        // aggregated handle: the finalize split must still fan out to every range to merge.
        if (!handle.isAggregated()) {
            ranges = pruneToBoundPartitionKey(
                    handle, ranges, constraint, config.splitPruningEnabled(), Partitioner.resolve());
        }

        // Aggregated handle: Trino does not re-aggregate across splits, so return
        // ONE finalize split carrying all range→replica assignments. Its PageSource
        // fans out, pulls each range's partial, merges, and emits the final rows.
        if (handle.isAggregated()) {
            CqliteFlightAggregateSplit finalize = new CqliteFlightAggregateSplit(
                    handle.keyspace(),
                    handle.table(),
                    handle.ddl(),
                    ranges,
                    handle.filterJson(),
                    handle.aggregationJson().orElseThrow(),
                    handle.finalizePlanJson().orElseThrow());
            return new FixedSplitSource(List.of(finalize));
        }

        return new FixedSplitSource(ranges);
    }

    /**
     * Prune {@code ranges} to those covering the bound partition key's Murmur3 token(s)
     * (issue #2679), or return {@code ranges} unchanged (full fan-out) on ANY doubt.
     *
     * <p>Fail-safe by construction: pruning is skipped — and the reason logged — when the
     * toggle is off, the resolved {@link Partitioner} is not Murmur3, the constraint does not
     * fully bind the partition key, a value cannot be serialized, the covering set would be
     * empty, or anything throws. Full fan-out is always correct, so a skip is never a
     * correctness risk; a mis-prune would be, hence the conservative posture.
     */
    static List<CqliteFlightSplit> pruneToBoundPartitionKey(
            CqliteFlightTableHandle handle, List<CqliteFlightSplit> ranges, Constraint constraint,
            boolean pruningEnabled, Partitioner partitioner) {
        try {
            if (!pruningEnabled) {
                return ranges;
            }
            if (constraint == null) {
                return ranges;
            }
            if (partitioner != Partitioner.MURMUR3) {
                LOG.log(System.Logger.Level.DEBUG,
                        () -> "Split pruning disabled: partitioner " + partitioner
                                + " is not Murmur3Partitioner; using full fan-out for "
                                + handle.keyspace() + "." + handle.table());
                return ranges;
            }
            List<KeyColumn> partitionKey = PrimaryKeyExtractor.extract(handle.ddl()).partitionKey();
            // Fail-safe composite-PK consistency guard (issue #2679 roborev): the pruning path
            // builds a composite partition-key byte layout whose Murmur3 token is correct ONLY
            // when EVERY partition-key component is present, in order. PrimaryKeyExtractor was
            // built for estimateGroupRatio (#944) where a parse MISS is fail-safe — but here an
            // UNDER-count on a composite PK (e.g. a quoted identifier containing ')' closes the
            // composite group early, so PRIMARY KEY ((a,b)) resolves to [a]) is CATASTROPHIC:
            // it builds a single-component raw-byte key, computes the WRONG token, maps to the
            // wrong range, and SILENTLY DROPS the partition's rows. Cross-check the extracted
            // partition-key column count against an INDEPENDENT, quote-aware arity scan of the
            // same DDL; prune only when they agree. Any disagreement — or an indeterminate
            // arity — means we cannot be certain we have all components in the right order, so
            // we fall back to the full fan-out (always correct). This subsumes the
            // single-component case (1 == 1) unchanged.
            java.util.OptionalInt arity = PrimaryKeyExtractor.partitionKeyArity(handle.ddl());
            if (arity.isEmpty() || arity.getAsInt() != partitionKey.size()) {
                LOG.log(System.Logger.Level.DEBUG,
                        () -> "Split pruning skipped for " + handle.keyspace() + "." + handle.table()
                                + " (full fan-out): partition-key parse is not provably complete — "
                                + "extractor resolved " + partitionKey.size() + " component(s) but the "
                                + "independent quote-aware arity scan found "
                                + (arity.isEmpty() ? "an indeterminate count" : arity.getAsInt())
                                + "; refusing to prune on a possibly under-counted composite key");
                return ranges;
            }
            BoundPartitionKeys bound = BoundPartitionKeys.compute(constraint.getSummary(), partitionKey);
            if (!bound.isBound()) {
                LOG.log(System.Logger.Level.DEBUG,
                        () -> "Split pruning skipped for " + handle.keyspace() + "." + handle.table()
                                + " (full fan-out): " + bound.skipReason());
                return ranges;
            }
            long[] tokens = bound.tokens().orElseThrow();
            List<CqliteFlightSplit> pruned = new ArrayList<>();
            for (CqliteFlightSplit split : ranges) {
                if (rangeCoversAnyToken(split, tokens)) {
                    pruned.add(split);
                }
            }
            if (pruned.isEmpty()) {
                // Every token fell outside every emitted range: the ring guard already
                // validated exact tiling, so this should be impossible — but if it happens,
                // fail safe to the full fan-out rather than return zero splits (0 rows).
                LOG.log(System.Logger.Level.WARNING,
                        () -> "Split pruning produced no covering range for a fully-bound key on "
                                + handle.keyspace() + "." + handle.table()
                                + "; falling back to full fan-out (never drop rows)");
                return ranges;
            }
            List<CqliteFlightSplit> full = ranges;
            LOG.log(System.Logger.Level.DEBUG,
                    () -> "Split pruning: " + full.size() + " -> " + pruned.size()
                            + " splits for a fully-bound partition key on "
                            + handle.keyspace() + "." + handle.table());
            return pruned;
        } catch (RuntimeException e) {
            // Any unexpected failure in the pruning path must NOT fail the query or drop rows.
            LOG.log(System.Logger.Level.WARNING,
                    "Split pruning failed; using full fan-out for "
                            + handle.keyspace() + "." + handle.table(), e);
            return ranges;
        }
    }

    /** Does this split's {@code (start, end]} range (with wraparound) contain any of {@code tokens}? */
    private static boolean rangeCoversAnyToken(CqliteFlightSplit split, long[] tokens) {
        for (long token : tokens) {
            if (Murmur3Token.tokenInRange(
                    token, split.tokenStart(), split.tokenEnd(), split.wraparound())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Fail-closed ring-coverage guard (issue #2237). The Sidecar's read-replica ranges
     * are expected to tile the Murmur3 token ring (signed 64-bit, a circle) EXACTLY once,
     * but during topology transitions (bootstrap/decommission) they can transiently OVERLAP
     * (silent duplicate rows) or GAP (silent missing rows). This verifies the exact tiling
     * before any split is built and throws an actionable {@link TrinoException} otherwise.
     *
     * <p>Ranges are {@code (start, end]} — start exclusive, end inclusive (Cassandra
     * convention, see {@link ReplicaInfo}); a range with {@code start >= end} wraps the ring
     * boundary. A full tiling is accepted in EITHER of the two forms the Sidecar emits:
     *
     * <ul>
     *   <li><b>Unwrapped form</b> — the form the real Sidecar emits (see
     *       {@code SidecarClientTest#parsesTokenRangeReplicasAndTokens}: {@code (MIN,0]} +
     *       {@code (0,MAX]}): sorted ranges whose FIRST start is {@link Long#MIN_VALUE} and
     *       whose LAST end is {@link Long#MAX_VALUE}, with no range wrapping. The chain
     *       {@code (MIN, …] … (…, MAX]} spans the whole ring exactly once.</li>
     *   <li><b>Wrap form</b> — a single range wraps the boundary and closes the circle: the
     *       last (highest-start) range wraps ({@code start >= end}) and its inclusive end
     *       equals the first range's exclusive start. A single {@code (T, T]} (issue #2228)
     *       is the degenerate one-range case of this form (the whole ring).</li>
     * </ul>
     *
     * <p>Both forms first require the interior to join perfectly: after sorting by start,
     * each range's {@code end} equals the NEXT range's {@code start} (for adjacent
     * {@code (a, b]} and {@code (b, c]} the shared token {@code b} is the inclusive end of
     * the first and the exclusive start of the second — no overlap, no gap). Any deviation
     * — interior, or at the ring-closing boundary — is classified via a signed token
     * comparison as an overlap (double coverage → duplicate rows) or a gap (uncovered
     * tokens → missing rows) and named in the error. Because at most ONE range may wrap and
     * it must be the closing (last) range, more than one wrapping range, or a wrapping /
     * full-ring range anywhere but last, is rejected as an overlap (it double-covers the
     * ring). {@code null} or an empty list fails closed (would silently return 0 rows).
     */
    static void validateRingCoverage(List<ReplicaInfo> ranges) {
        if (ranges == null || ranges.isEmpty()) {
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Cassandra token-range topology returned no read-replica ranges, so the "
                            + "token ring is not covered; refusing to scan (would silently return "
                            + "0 rows). This is usually a transient topology transition — retry.");
        }
        List<ReplicaInfo> sorted = new ArrayList<>(ranges);
        sorted.sort(Comparator.comparingLong(ReplicaInfo::startToken));
        int n = sorted.size();

        // A valid tiling has AT MOST ONE wrapping range (start >= end), and it must be the
        // closing (last, highest-start) range. Two wrapping ranges, or a wrap / full-ring
        // (T,T] range anywhere but last, necessarily double-covers the ring -> overlap. This
        // also closes a false-accept: an interior (T,T] full-ring range whose neighbours
        // share its token would otherwise pass the adjacency check below.
        int wrapCount = 0;
        int lastWrapIndex = -1;
        for (int i = 0; i < n; i++) {
            ReplicaInfo r = sorted.get(i);
            if (r.startToken() >= r.endToken()) {
                wrapCount++;
                lastWrapIndex = i;
            }
        }
        if (wrapCount > 1 || (wrapCount == 1 && lastWrapIndex != n - 1)) {
            ReplicaInfo w = sorted.get(lastWrapIndex);
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Cassandra token-range topology does not tile the ring exactly once: "
                            + "overlap — a ring-wrapping range (" + w.start() + ", " + w.end()
                            + "] is not the single closing range, so it double-covers the ring. "
                            + "This risks duplicate rows and is usually a transient topology "
                            + "transition (bootstrap/decommission) — retry.");
        }

        // Interior adjacency: each range's end must meet the next range's start exactly.
        // Iterates consecutive pairs only (does NOT wrap the last back to the first); the
        // ring is closed separately below so both coverage forms are handled.
        for (int i = 0; i + 1 < n; i++) {
            ReplicaInfo cur = sorted.get(i);
            ReplicaInfo next = sorted.get(i + 1);
            if (cur.endToken() != next.startToken()) {
                throw tilingFault(cur, next, cur.endToken(), next.startToken());
            }
        }

        ReplicaInfo first = sorted.get(0);
        ReplicaInfo last = sorted.get(n - 1);
        if (wrapCount == 0) {
            // Unwrapped chain (first.start, last.end] — no range crosses the boundary.
            // (a) Full coverage iff it spans (MIN, MAX].
            if (first.startToken() == Long.MIN_VALUE && last.endToken() == Long.MAX_VALUE) {
                return;
            }
            // Otherwise the ring boundary (past last.end, around to first.start) is uncovered
            // — always a GAP; a non-wrapping chain can never double-cover the boundary.
            throw ringBoundaryGap(last, first);
        }
        // wrapCount == 1 and it is the closing (last) range. (b) The circle closes iff the
        // wrap's inclusive end meets the first range's exclusive start (subsumes the single
        // (T, T] full ring). Otherwise the wrap end PAST first.start double-covers (overlap)
        // or falls SHORT (gap) — the signed comparison classifies correctly here.
        if (last.endToken() == first.startToken()) {
            return;
        }
        throw tilingFault(last, first, last.endToken(), first.startToken());
    }

    /**
     * The ring-closing GAP for an unwrapped chain that does not span the full {@code (MIN, MAX]}
     * ring: the tokens past {@code last.end} around the boundary to {@code first.start} are owned
     * by no range (→ missing rows). Reported as a gap regardless of token magnitudes because a
     * non-wrapping chain cannot double-cover.
     */
    private static TrinoException ringBoundaryGap(ReplicaInfo last, ReplicaInfo first) {
        return new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                "Cassandra token-range topology does not tile the ring exactly once: gap at the "
                        + "ring boundary — the ranges cover only (" + first.start() + ", "
                        + last.end() + "] and no range wraps to close the circle, so tokens past "
                        + last.end() + " (around to " + first.start() + ") are uncovered. This "
                        + "risks missing rows and is usually a transient topology transition "
                        + "(bootstrap/decommission) — retry.");
    }

    /**
     * Build the actionable typed error for a ring-tiling fault, classifying it as an overlap
     * (double coverage → duplicate rows) when {@code curEnd} is past {@code nextStart}, else a
     * gap (uncovered tokens → missing rows), naming the offending boundary.
     */
    private static TrinoException tilingFault(ReplicaInfo cur, ReplicaInfo next, long curEnd, long nextStart) {
        String kind = Long.compare(curEnd, nextStart) > 0 ? "overlap" : "gap";
        return new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                "Cassandra token-range topology does not tile the ring exactly once: "
                        + kind + " between range (" + cur.start() + ", " + cur.end() + "] and "
                        + "range (" + next.start() + ", " + next.end() + "] — range 1 ends at "
                        + "token " + curEnd + " but range 2 starts at token " + nextStart
                        + ". This risks "
                        + ("overlap".equals(kind) ? "duplicate" : "missing")
                        + " rows and is usually a transient topology transition "
                        + "(bootstrap/decommission) — retry.");
    }

    /**
     * Backward-compatible overload for a live-dir scan (no snapshot). Delegates with
     * {@link Optional#empty()}.
     */
    public static List<CqliteFlightSplit> buildSplits(
            CqliteFlightTableHandle table,
            TokenRangeReplicasResponse replicas,
            String localDatacenter,
            int flightPort) {
        return buildSplits(table, replicas, localDatacenter, flightPort, Optional.empty());
    }

    /**
     * Convenience overload without explicit per-host snapshot-availability data: defaults the
     * fallback-availability set to {@link #distinctReplicaHosts} (primaries only) — a
     * conservative default for callers that stamp a snapshot name without wiring the real
     * per-host availability. The live query path ({@link #getSplits}) always calls the 6-arg
     * overload below with the FULL replica-owner availability set (issue #2241), which is NOT
     * restricted to primaries.
     */
    public static List<CqliteFlightSplit> buildSplits(
            CqliteFlightTableHandle table,
            TokenRangeReplicasResponse replicas,
            String localDatacenter,
            int flightPort,
            Optional<String> snapshot) {
        return buildSplits(table, replicas, localDatacenter, flightPort, snapshot,
                distinctReplicaHosts(replicas, localDatacenter));
    }

    /**
     * Pure split-building: one split per read-replica token range, each pinned to a single
     * deterministically-chosen replica, all stamped with the same {@code snapshot} (issue
     * #2105). Static for unit testing without a live Sidecar or Trino session.
     *
     * <p>Availability failover (issue #2241): each split carries an ORDERED replica list
     * (primary first, then the range's other owners) that the page source retries on a
     * connect-class failure. In SNAPSHOT mode a fallback is only usable if its host is in
     * {@code availableSnapshotHosts} — the set of hosts whose snapshot creation actually
     * succeeded ({@link #getSplits} computes this over the FULL replica-owner union, not just
     * primaries, so a fallback-only host is eligible whenever its own snapshot creation
     * succeeded). In LIVE mode ({@code snapshot} empty) every replica owner is eligible and
     * {@code availableSnapshotHosts} is ignored.
     */
    public static List<CqliteFlightSplit> buildSplits(
            CqliteFlightTableHandle table,
            TokenRangeReplicasResponse replicas,
            String localDatacenter,
            int flightPort,
            Optional<String> snapshot,
            Set<String> availableSnapshotHosts) {
        // K=1 delegating overload: one split per range, byte-identical to the pre-#2680 behavior.
        return buildSplits(table, replicas, localDatacenter, flightPort, snapshot,
                availableSnapshotHosts, 1);
    }

    /**
     * Weight-balanced split-building (issue #2680): deterministically sub-split each read-replica
     * token range into {@code subSplitsPerRange} equal-token-span slices at THIS single seam,
     * then build one split per slice. Each slice inherits its parent range's owner set, its
     * primary is the consecutive owner after the parent's rotated head (slice {@code i} →
     * {@code rotated(parent)[i % n]}, {@link #orderedReplicaHostsForSlice}) so per-owner slice
     * counts within a range differ by at most one and per-owner total token span converges to
     * ~1/N of the total, and each split's {@link CqliteFlightSplit#getSplitWeight()} is
     * proportional to its slice's token span (mean-span slice = standard). With
     * {@code subSplitsPerRange == 1} this reduces to the pre-#2680 one-split-per-range behavior
     * exactly.
     */
    public static List<CqliteFlightSplit> buildSplits(
            CqliteFlightTableHandle table,
            TokenRangeReplicasResponse replicas,
            String localDatacenter,
            int flightPort,
            Optional<String> snapshot,
            Set<String> availableSnapshotHosts,
            int subSplitsPerRange) {
        List<SlicedRange> sliced = sliceRanges(replicas.readReplicas(), subSplitsPerRange);
        // Collect the emitted (slice, host, fallbacks) tuples and the total token span over ONLY
        // emitted slices, so the mean-span weight scale (mean slice = standard) is exact.
        List<EmittedSlice> emitted = new ArrayList<>();
        BigInteger totalSpan = BigInteger.ZERO;
        for (SlicedRange sr : sliced) {
            // Sidecar returns replicas as "ip:storage_port"; the flight server listens on
            // flightPort at the same host, so keep only the host. The ordered list's first entry
            // is this slice's rotated primary (issue #2680 spreads slices across the parent's owners).
            List<String> ordered = orderedReplicaHostsForSlice(
                    sr.replicasByDatacenter(), localDatacenter, sr.parentRotationKey(), sr.sliceIndex());
            if (ordered.isEmpty()) {
                continue; // slice of a range with no known replica — nothing to read
            }
            String host = ordered.get(0);
            List<String> fallbacks = new ArrayList<>();
            for (int i = 1; i < ordered.size(); i++) {
                String candidate = ordered.get(i);
                if (snapshot.isEmpty() || availableSnapshotHosts.contains(candidate)) {
                    fallbacks.add(candidate);
                }
            }
            BigInteger span = TokenRangeSlicer.span(sr.start(), sr.end());
            totalSpan = totalSpan.add(span);
            emitted.add(new EmittedSlice(sr.start(), sr.end(), host, fallbacks, span));
        }
        int numSlices = emitted.size();
        List<CqliteFlightSplit> splits = new ArrayList<>(numSlices);
        for (EmittedSlice es : emitted) {
            // #2228: equal endpoints (start == end) denote the FULL ring — the Cassandra convention
            // for a range `(T, T]` — not the empty set (the slicer never emits an empty slice).
            boolean wraparound = es.start() >= es.end();
            double weight = weightProportion(es.span(), totalSpan, numSlices);
            splits.add(new CqliteFlightSplit(
                    table.keyspace(),
                    table.table(),
                    table.ddl(),
                    es.host(),
                    flightPort,
                    es.start(),
                    es.end(),
                    wraparound,
                    snapshot,
                    es.fallbacks(),
                    weight));
        }
        return splits;
    }

    /**
     * A slice's {@link CqliteFlightSplit#weightProportion()} (issue #2680): its token span
     * relative to the mean slice span ({@code span * numSlices / totalSpan}), so a mean-span slice
     * is {@code 1.0} (= {@link io.trino.spi.SplitWeight#standard()}). Clamping to Trino's valid
     * range happens in the split's constructor. Degenerate inputs degrade to the standard weight.
     */
    private static double weightProportion(BigInteger span, BigInteger totalSpan, int numSlices) {
        if (numSlices <= 0 || totalSpan.signum() == 0) {
            return CqliteFlightSplit.STANDARD_WEIGHT_PROPORTION;
        }
        return span.doubleValue() * numSlices / totalSpan.doubleValue();
    }

    /**
     * Expand each read-replica range into {@code subSplitsPerRange} equal-token-span slices
     * (issue #2680) — the SINGLE slicing seam every consumer (scan split construction, the
     * snapshot host chooser) draws from, so slicing, rotation, and weight are reasoned about once.
     * Each {@link SlicedRange} carries the parent's start token (the rotation key) and the slice's
     * index within the parent so the primary lands on the consecutive rotated owner.
     */
    static List<SlicedRange> sliceRanges(List<ReplicaInfo> ranges, int subSplitsPerRange) {
        List<SlicedRange> out = new ArrayList<>();
        for (ReplicaInfo range : ranges) {
            List<TokenRangeSlicer.Slice> slices =
                    TokenRangeSlicer.slice(range.startToken(), range.endToken(), subSplitsPerRange);
            for (int i = 0; i < slices.size(); i++) {
                TokenRangeSlicer.Slice sl = slices.get(i);
                out.add(new SlicedRange(
                        sl.start(), sl.end(), range.replicasByDatacenter(), range.startToken(), i));
            }
        }
        return out;
    }

    /**
     * One equal-span slice of a parent read-replica range (issue #2680): its half-open
     * {@code (start, end]} tokens, the parent's owner set (inherited verbatim), the parent's
     * start token as the rotation key, and the slice's index within the parent (0-based) so the
     * primary is chosen as the consecutive rotated owner.
     */
    record SlicedRange(
            long start,
            long end,
            Map<String, List<String>> replicasByDatacenter,
            long parentRotationKey,
            int sliceIndex) {}

    /** A slice's emitted split data plus its token span, for the two-pass weight computation. */
    private record EmittedSlice(long start, long end, String host, List<String> fallbacks, BigInteger span) {}

    /**
     * The range's replica hosts in try order (issues #2241, #2397): the primary first, then
     * the range's other owners as ordered availability fallbacks, mapped to bare hosts via
     * {@link #hostOnly} and de-duplicated preserving order. The first element equals
     * {@link #pickReplica}'s choice.
     *
     * <p>Primary selection is a deterministic per-range ROTATION over the sorted eligible
     * owner set, not the lexicographic head (issue #2397): with RF == N every range shares
     * one identical owner set, so a fixed head pinned EVERY split to the same replica and
     * collapsed all flight reads onto a single pod (round-9 field finding). The primary is
     * {@code sorted[floorMod(rotationKey, size)]} (a stable per-range value — the range's
     * start token) and the remaining owners follow in sorted order as fallbacks. Rotation is
     * applied WITHIN the local-datacenter owner set when that DC has replicas for the range
     * (datacenter preference preserved), else across the whole owner set; owners in other
     * datacenters are appended, sorted, as further fallbacks.
     */
    static List<String> orderedReplicaHosts(
            Map<String, List<String>> replicasByDatacenter, String localDatacenter, long rotationKey) {
        // Slice index 0 is the parent's rotated head, so this is byte-identical to the pre-#2680
        // (pre-slicing) ordering — the K=1 identity for a whole range.
        return orderedReplicaHostsForSlice(replicasByDatacenter, localDatacenter, rotationKey, 0);
    }

    /**
     * The slice's replica hosts in try order (issues #2241, #2397, #2680): its primary first, then
     * the parent range's other owners as ordered availability fallbacks (full owner set retained),
     * mapped to bare hosts via {@link #hostOnly} and de-duplicated preserving order.
     *
     * <p>Weight-balanced slice→owner assignment (issue #2680): the primary is the consecutive owner
     * after the PARENT range's rotated head — slice {@code i}'s head index into the sorted eligible
     * owners is {@code (floorMod(parentRotationKey, n) + sliceIndex) mod n}. So within one parent,
     * consecutive slices land on consecutive owners (per-owner slice counts differ by at most one),
     * and the remainder owner varies with the parent's rotation key, spreading remainders across
     * ranges — per-owner total token span converges to ~1/n independent of inter-range weight skew.
     * Slice index 0 is exactly the parent's rotated head, so K=1 reproduces the pre-#2680 ordering.
     * Rotation is applied WITHIN the local-datacenter owner set when that DC owns the range (DC
     * preference preserved), else across the whole owner set; other datacenters are appended sorted.
     */
    static List<String> orderedReplicaHostsForSlice(
            Map<String, List<String>> replicasByDatacenter, String localDatacenter,
            long parentRotationKey, int sliceIndex) {
        List<String> primaryGroup;
        List<String> tailGroup;
        List<String> local = localDatacenter != null ? replicasByDatacenter.get(localDatacenter) : null;
        if (local != null && !local.isEmpty()) {
            // Rotate within the local DC; other DCs are fallback-only, appended sorted.
            primaryGroup = local.stream().sorted().collect(java.util.stream.Collectors.toList());
            tailGroup = replicasByDatacenter.entrySet().stream()
                    .filter(e -> !e.getKey().equals(localDatacenter))
                    .flatMap(e -> e.getValue().stream())
                    .sorted()
                    .collect(java.util.stream.Collectors.toList());
        } else {
            // No local-DC preference for this range: the whole owner set is rotation-eligible.
            primaryGroup = replicasByDatacenter.values().stream()
                    .flatMap(List::stream)
                    .sorted()
                    .collect(java.util.stream.Collectors.toList());
            tailGroup = List.of();
        }
        int n = primaryGroup.size();
        // Consecutive-owner head for this slice; sliceIndex is small (< K <= 64), no overflow.
        int head = n <= 1 ? 0 : Math.floorMod(Math.floorMod(parentRotationKey, n) + sliceIndex, n);
        List<String> orderedAddresses = new ArrayList<>(rotate(primaryGroup, head));
        orderedAddresses.addAll(tailGroup);
        List<String> hosts = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (String address : orderedAddresses) {
            String host = hostOnly(address);
            if (seen.add(host)) {
                hosts.add(host);
            }
        }
        return hosts;
    }

    /**
     * Deterministic per-range rotation of a sorted owner list (issue #2397): the head is
     * {@code sorted[floorMod(rotationKey, n)]}, the remaining entries follow in their natural
     * sorted order. Stable for a given {@code rotationKey} (the range start token), so splits
     * are reproducible across re-planning while spreading primaries across owners. A list of
     * size &le; 1 is returned unchanged.
     */
    private static List<String> rotate(List<String> sorted, long rotationKey) {
        int n = sorted.size();
        if (n <= 1) {
            return sorted;
        }
        int head = Math.floorMod(rotationKey, n);
        List<String> rotated = new ArrayList<>(n);
        rotated.add(sorted.get(head));
        for (int i = 0; i < n; i++) {
            if (i != head) {
                rotated.add(sorted.get(i));
            }
        }
        return rotated;
    }

    /**
     * The distinct set of replica hosts the scan's splits will read — the same deterministic
     * rotated {@link #pickReplica} choice used by {@link #buildSplits}, one host per range,
     * deduplicated (issue #2397 routes this through the rotated chooser so every pinned
     * primary has its snapshot). Order-preserving (insertion order) so per-host snapshot
     * creation is stable across re-planning. This is exactly the set of hosts a per-query
     * snapshot must be created on (issue #2227).
     */
    static Set<String> distinctReplicaHosts(TokenRangeReplicasResponse replicas, String localDatacenter) {
        // K=1 delegating overload: one primary per whole range (pre-#2680 behavior).
        return distinctReplicaHosts(replicas, localDatacenter, 1);
    }

    /**
     * The distinct set of replica hosts the scan's splits will read at SLICE granularity (issues
     * #2227, #2680): the same rotated chooser used by {@link #buildSplits}, one primary per SLICE,
     * deduplicated (insertion order, stable across re-planning). Because slicing happens at the
     * single {@link #sliceRanges} seam, this equals exactly the set of per-slice primary hosts the
     * splits pin — so a per-query snapshot is created on every host a split will read (fail-closed,
     * #2227) even after weight-balanced sub-splitting spreads primaries across a range's owners.
     */
    static Set<String> distinctReplicaHosts(
            TokenRangeReplicasResponse replicas, String localDatacenter, int subSplitsPerRange) {
        Set<String> hosts = new LinkedHashSet<>();
        for (SlicedRange sr : sliceRanges(replicas.readReplicas(), subSplitsPerRange)) {
            // pickReplicaForSlice returns the slice's rotated primary already as a bare host (via
            // orderedReplicaHostsForSlice), so it is added directly — no second port strip.
            String host = pickReplicaForSlice(
                    sr.replicasByDatacenter(), localDatacenter, sr.parentRotationKey(), sr.sliceIndex());
            if (host != null) {
                hosts.add(host);
            }
        }
        return hosts;
    }

    /**
     * The union of every replica host that owns ANY token range the scan will read (issue
     * #2241) — primaries AND every other owner, per range via {@link #orderedReplicaHosts},
     * deduplicated across ranges (insertion order). This is the full candidate set for
     * best-effort per-host snapshot creation, so an availability-failover fallback is not
     * silently restricted to {@link #distinctReplicaHosts} (primaries only): a fallback host
     * that is never any range's primary must still be eligible when its own snapshot creation
     * succeeds.
     */
    static Set<String> allReplicaHosts(TokenRangeReplicasResponse replicas, String localDatacenter) {
        Set<String> hosts = new LinkedHashSet<>();
        for (ReplicaInfo range : replicas.readReplicas()) {
            // The full owner union is rotation-independent (order within a range does not
            // change the set); pass the range's start token to match orderedReplicaHosts.
            hosts.addAll(orderedReplicaHosts(range.replicasByDatacenter(), localDatacenter, range.startToken()));
        }
        return hosts;
    }

    /**
     * Choose one replica for a range: prefer the local datacenter, else any DC. Selection is
     * a deterministic per-range ROTATION over the sorted eligible owners (issue #2397) — the
     * head of {@link #orderedReplicaHosts} — so repeated planning yields stable splits AND
     * primaries spread across owners instead of all pinning to the lexicographic head (which
     * collapsed every RF==N range onto one replica). Returns {@code null} for a range with no
     * known replica.
     */
    static String pickReplica(
            Map<String, List<String>> replicasByDatacenter, String localDatacenter, long rotationKey) {
        return pickReplicaForSlice(replicasByDatacenter, localDatacenter, rotationKey, 0);
    }

    /**
     * Choose the rotated primary for one SLICE (issue #2680): the head of
     * {@link #orderedReplicaHostsForSlice}, i.e. the consecutive owner after the parent's rotated
     * head. Returns {@code null} for a range with no known replica.
     */
    static String pickReplicaForSlice(
            Map<String, List<String>> replicasByDatacenter, String localDatacenter,
            long parentRotationKey, int sliceIndex) {
        List<String> ordered = orderedReplicaHostsForSlice(
                replicasByDatacenter, localDatacenter, parentRotationKey, sliceIndex);
        return ordered.isEmpty() ? null : ordered.get(0);
    }

    /**
     * Normalize an {@code ip:port} (or bracketed IPv6) replica address to a bare host
     * literal via the single {@link HostAddresses} authority, so the split's pinned host
     * and the per-host snapshot URI (issue #2227) agree on the exact host string.
     */
    static String hostOnly(String address) {
        return HostAddresses.hostOnly(address);
    }
}

