package in.mcfad.cqlite.flight;

import java.util.Locale;

/**
 * How the connector resolves the SSTable file set each scan reads (issue #2105).
 *
 * <p>The cqlite-flight server compaction-merges a Cassandra node's SSTables on
 * every read. Which files it sees depends on this mode:
 *
 * <ul>
 *   <li>{@link #SNAPSHOT} (default) — before planning a scan the connector creates
 *       a Cassandra Sidecar snapshot (a hard-linked, immutable copy of the current
 *       SSTable set) and names it in the Flight ticket, so every split reads a
 *       <em>stable</em> file set even while Cassandra compacts and flushes
 *       underneath. Bounded staleness (data as of snapshot time), consistent read.</li>
 *   <li>{@link #LIVE} — the ticket names no snapshot; the server reads the live data
 *       directory. Always the most current flushed data, but a long scan
 *       <em>races compaction</em> (files can be removed mid-scan). This is the
 *       stress-hunting mode.</li>
 * </ul>
 */
public enum ReadMode {
    SNAPSHOT,
    LIVE;

    /** Parse a catalog property value (case-insensitive); blank/null → {@link #SNAPSHOT}. */
    public static ReadMode fromConfig(String value) {
        if (value == null || value.isBlank()) {
            return SNAPSHOT;
        }
        try {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid value for cqlite.read-mode: '" + value + "' (expected snapshot or live)");
        }
    }
}
