package in.mcfad.cqlite.flight;

/**
 * The cluster partitioner, resolved for plan-time split pruning (issue #2679).
 *
 * <p>Under the no-heuristics mandate the token is computed from schema-declared
 * partition-key columns against a <em>declared</em> partitioner — never inferred
 * from data. The Cassandra Sidecar exposes no partitioner field today, and the
 * entire ring machinery ({@code CqliteFlightSplitManager.validateRingCoverage},
 * {@code SidecarModels}, token parsing) already <b>hard-assumes
 * {@code Murmur3Partitioner}</b> (signed 64-bit tokens). This enum makes that
 * assumption explicit and centralized: {@link #resolve()} returns {@link #MURMUR3}
 * (the current and only supported case). If/when a partitioner name becomes
 * available from Sidecar metadata, {@link #resolve()} reads it; any non-Murmur3 or
 * unknown partitioner resolves to {@link #UNSUPPORTED}, which disables pruning
 * (full fan-out is always correct) — see {@code CqliteFlightSplitManager}.
 *
 * <p>This is an OWNER-APPROVED design decision (design.md §2): the honest encoding
 * of today's reality, fully fail-safe. Follow-up: surface the partitioner name from
 * Sidecar when that metadata lands.
 */
public enum Partitioner {
    /** Cassandra's {@code Murmur3Partitioner} — the only partitioner that prunes. */
    MURMUR3,
    /** Any unknown or non-Murmur3 partitioner — pruning is disabled (full fan-out). */
    UNSUPPORTED;

    /**
     * The partitioner assumed for the ring today. The ring already treats all token
     * bounds as signed 64-bit Murmur3 tokens, so this returns {@link #MURMUR3}. This
     * is the single resolution point a future Sidecar-metadata source would read.
     */
    public static Partitioner resolve() {
        return MURMUR3;
    }
}
