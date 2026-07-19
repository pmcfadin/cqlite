package in.mcfad.cqlite.flight;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import in.mcfad.cqlite.flight.PrimaryKeyExtractor.KeyColumn;

/**
 * Classifies whether a pushed-down {@link Constraint} summary fully binds the
 * partition key and, if so, computes the Murmur3 token(s) of the bound key(s) for
 * plan-time split pruning (issue #2679).
 *
 * <p>The partition key is <b>fully bound</b> iff <em>every</em> partition-key column
 * (by {@link PrimaryKeyExtractor} name + order, case-folded per {@link KeyColumn})
 * appears in the summary with a domain that is either a <b>single value</b> (equality)
 * or a <b>discrete set</b> (IN) — and null is not allowed. The bound keys are the
 * Cartesian product of each column's value list; a token is computed per full key and
 * the distinct set returned.
 *
 * <p><b>Fail-safe (spec / no-heuristics):</b> a partial PK, a PK column bound by a
 * range/unbounded domain, a null-allowed domain, an empty product, or a value with no
 * exact CQL byte serialization ({@link PartitionKeyBytes}) all yield {@link #tokens()}
 * empty — the caller then emits the full fan-out (always correct) and logs
 * {@link #skipReason()}. Never fewer splits than correctness requires.
 */
public record BoundPartitionKeys(Optional<long[]> tokens, String skipReason) {

    /**
     * Upper bound on the enumerated Cartesian product of full keys. Beyond this, pruning
     * is skipped (full fan-out) — a huge IN over composite keys is atypical and not worth
     * the plan-time cost; skipping is always correct.
     */
    static final int MAX_KEYS = 10_000;

    private static BoundPartitionKeys prune(long[] tokens) {
        return new BoundPartitionKeys(Optional.of(tokens), "");
    }

    private static BoundPartitionKeys noPrune(String reason) {
        return new BoundPartitionKeys(Optional.empty(), reason);
    }

    /** True when the partition key is fully bound and tokens were computed. */
    public boolean isBound() {
        return tokens.isPresent();
    }

    /**
     * Compute the distinct covering tokens for {@code summary} against the partition-key
     * columns {@code partitionKey}, or a no-prune result (with a reason) when the key is
     * not fully bound / not serializable.
     */
    public static BoundPartitionKeys compute(TupleDomain<ColumnHandle> summary, List<KeyColumn> partitionKey) {
        if (partitionKey.isEmpty()) {
            return noPrune("no partition-key columns resolved from the DDL");
        }
        if (summary == null || summary.isAll() || summary.isNone()) {
            return noPrune("constraint summary binds no columns");
        }
        Optional<Map<ColumnHandle, Domain>> domainsOpt = summary.getDomains();
        if (domainsOpt.isEmpty()) {
            return noPrune("constraint summary has no column domains");
        }
        Map<ColumnHandle, Domain> domains = domainsOpt.get();

        // For each PK column (in schema order) collect its value bytes per bound value.
        // Each entry is the list of value-byte alternatives for that column (equality → 1,
        // IN → N). The Cartesian product across columns enumerates the full keys.
        List<List<byte[]>> perColumn = new ArrayList<>(partitionKey.size());
        for (KeyColumn pk : partitionKey) {
            ColumnDomain cd = findDomain(domains, pk);
            if (cd == null) {
                return noPrune("partition-key column '" + pk.name() + "' is not bound by the constraint");
            }
            Domain domain = cd.domain();
            if (domain.isNullAllowed()) {
                return noPrune("partition-key column '" + pk.name() + "' domain allows NULL");
            }
            ValueSet values = domain.getValues();
            Type type = domain.getType();
            List<Object> raw;
            if (values.isSingleValue()) {
                raw = List.of(values.getSingleValue());
            } else if (values.isDiscreteSet()) {
                raw = values.getDiscreteSet();
                if (raw.isEmpty()) {
                    return noPrune("partition-key column '" + pk.name() + "' has an empty discrete set");
                }
            } else {
                return noPrune("partition-key column '" + pk.name()
                        + "' is bound by a range/unbounded domain, not equality/IN");
            }
            List<byte[]> componentBytes = new ArrayList<>(raw.size());
            for (Object value : raw) {
                Optional<byte[]> bytes = PartitionKeyBytes.serializeValue(type, cd.capability(), value);
                if (bytes.isEmpty()) {
                    return noPrune("partition-key column '" + pk.name()
                            + "' value cannot be serialized to CQL partition-key bytes (type "
                            + type.getDisplayName() + ")");
                }
                componentBytes.add(bytes.get());
            }
            perColumn.add(componentBytes);
        }

        // Cartesian product size guard (still correct if skipped).
        long product = 1;
        for (List<byte[]> col : perColumn) {
            product *= col.size();
            if (product > MAX_KEYS) {
                return noPrune("bound key enumeration exceeds " + MAX_KEYS + " keys; skipping pruning");
            }
        }

        // Enumerate the product → full-key bytes → distinct tokens.
        Set<Long> tokenSet = new LinkedHashSet<>();
        List<byte[]> current = new ArrayList<>(perColumn.size());
        boolean ok = enumerate(perColumn, 0, current, tokenSet);
        if (!ok || tokenSet.isEmpty()) {
            return noPrune("bound key produced no serializable full key");
        }
        long[] out = new long[tokenSet.size()];
        int i = 0;
        for (long t : tokenSet) {
            out[i++] = t;
        }
        return prune(out);
    }

    /** Recursively enumerate the Cartesian product, computing a token per full key. */
    private static boolean enumerate(
            List<List<byte[]>> perColumn, int depth, List<byte[]> current, Set<Long> out) {
        if (depth == perColumn.size()) {
            Optional<byte[]> key = PartitionKeyBytes.fullKey(current);
            if (key.isEmpty()) {
                return false;
            }
            out.add(Murmur3Token.token(key.get()));
            return true;
        }
        for (byte[] component : perColumn.get(depth)) {
            current.add(component);
            if (!enumerate(perColumn, depth + 1, current, out)) {
                return false;
            }
            current.remove(current.size() - 1);
        }
        return true;
    }

    /** A matched column domain with its pushdown capability (for VARCHAR text vs uuid disambiguation). */
    private record ColumnDomain(Domain domain, PushdownCapability capability) {}

    /**
     * Find the summary domain for a partition-key column, matching by CQL identifier rules
     * (case-folded per {@link KeyColumn#matches}). Only a {@link CqliteFlightColumnHandle}
     * carries the capability needed to serialize the value; an unknown handle is skipped.
     */
    private static ColumnDomain findDomain(Map<ColumnHandle, Domain> domains, KeyColumn pk) {
        for (Map.Entry<ColumnHandle, Domain> e : domains.entrySet()) {
            if (e.getKey() instanceof CqliteFlightColumnHandle col && pk.matches(col.name())) {
                return new ColumnDomain(e.getValue(), col.capability());
            }
        }
        return null;
    }
}
