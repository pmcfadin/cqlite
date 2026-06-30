package in.mcfad.cqlite.flight;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts the partition-key and clustering-column names from a single
 * {@code CREATE TABLE} DDL (issue #944). Used by
 * {@link CqliteFlightMetadata#estimateGroupRatio} to map a GROUP BY shape onto
 * the authoritative partition/row counts.
 *
 * <p>Handles both PRIMARY KEY spellings Cassandra emits:
 * <ul>
 *   <li>Inline single-column: {@code id int PRIMARY KEY} → partition key = [id],
 *       no clustering columns.</li>
 *   <li>Composite clause: {@code PRIMARY KEY ((pk1, pk2), ck1, ck2)} → partition
 *       key = [pk1, pk2], clustering = [ck1, ck2]; or
 *       {@code PRIMARY KEY (pk, ck1, ck2)} → partition key = [pk], clustering =
 *       [ck1, ck2].</li>
 * </ul>
 *
 * <p>This is a best-effort structural parse of the DDL the connector already
 * carries on the table handle — it never inspects data. When the PRIMARY KEY
 * cannot be located the key lists are empty and the caller falls back to the
 * safe default (push), so a parse miss is never a correctness risk.
 */
public final class PrimaryKeyExtractor {
    private PrimaryKeyExtractor() {}

    /** Resolved key columns of a table. */
    public record Keys(List<String> partitionKey, List<String> clusteringColumns) {
        /** Distinct names of partition + clustering columns, lower-cased. */
        public Set<String> allKeyColumns() {
            Set<String> all = new LinkedHashSet<>(partitionKey);
            all.addAll(clusteringColumns);
            return all;
        }
    }

    private static final Pattern PK_CLAUSE =
            Pattern.compile("(?is)PRIMARY\\s+KEY\\s*\\(");
    private static final Pattern INLINE_PK =
            Pattern.compile("(?is)\\bPRIMARY\\s+KEY\\b");

    /**
     * Parse the partition-key and clustering columns out of {@code ddl}. Returns
     * empty lists when no PRIMARY KEY can be located.
     */
    public static Keys extract(String ddl) {
        if (ddl == null) {
            return new Keys(List.of(), List.of());
        }
        Matcher clause = PK_CLAUSE.matcher(ddl);
        if (clause.find()) {
            return parseClause(ddl, clause.end() - 1);
        }
        // Inline single-column form: "<col> <type> PRIMARY KEY".
        Matcher inline = INLINE_PK.matcher(ddl);
        if (inline.find()) {
            String col = inlinePartitionColumn(ddl, inline.start());
            if (col != null) {
                return new Keys(List.of(col), List.of());
            }
        }
        return new Keys(List.of(), List.of());
    }

    /**
     * Parse {@code PRIMARY KEY ( ... )} starting at {@code openParen} (the index of
     * the opening parenthesis of the PRIMARY KEY clause).
     */
    private static Keys parseClause(String ddl, int openParen) {
        // Capture the balanced parenthesis body of the PRIMARY KEY clause.
        int depth = 0;
        int close = -1;
        for (int i = openParen; i < ddl.length(); i++) {
            char c = ddl.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    close = i;
                    break;
                }
            }
        }
        if (close < 0) {
            return new Keys(List.of(), List.of());
        }
        String body = ddl.substring(openParen + 1, close);

        // Split top-level (depth-0) commas; a leading "(...)" is the composite
        // partition key, everything after it is clustering columns.
        List<String> topLevel = new ArrayList<>();
        List<String> partitionKey = new ArrayList<>();
        boolean compositePk = body.trim().startsWith("(");
        int d = 0;
        StringBuilder cur = new StringBuilder();
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '(') {
                d++;
                if (!(compositePk && d == 1)) {
                    cur.append(c);
                }
            } else if (c == ')') {
                d--;
                if (compositePk && d == 0) {
                    // End of the composite partition-key group.
                    for (String p : cur.toString().split(",")) {
                        addName(partitionKey, p);
                    }
                    cur.setLength(0);
                } else {
                    cur.append(c);
                }
            } else if (c == ',' && d == 0) {
                topLevel.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        if (cur.length() > 0) {
            topLevel.add(cur.toString());
        }

        List<String> clustering = new ArrayList<>();
        if (compositePk) {
            // partitionKey already filled from the "(...)" group; topLevel holds the
            // clustering columns (the first topLevel entry may be empty from the
            // comma right after the closing ')').
            for (String t : topLevel) {
                addName(clustering, t);
            }
        } else {
            // Simple PK: first top-level name is the partition key, rest cluster.
            for (int i = 0; i < topLevel.size(); i++) {
                if (i == 0) {
                    addName(partitionKey, topLevel.get(i));
                } else {
                    addName(clustering, topLevel.get(i));
                }
            }
        }
        return new Keys(List.copyOf(partitionKey), List.copyOf(clustering));
    }

    /**
     * For the inline form, the partition-key column is the identifier immediately
     * preceding its type in the column definition that ends with PRIMARY KEY:
     * {@code "<name> <type> PRIMARY KEY"}. Walk back from the PRIMARY KEY keyword
     * to the start of the column definition (the previous top-level comma or open
     * paren) and take the first identifier.
     */
    private static String inlinePartitionColumn(String ddl, int primaryKeyStart) {
        int start = primaryKeyStart;
        int depth = 0;
        while (start > 0) {
            char c = ddl.charAt(start - 1);
            if (c == ')') {
                depth++;
            } else if (c == '(') {
                if (depth == 0) {
                    break;
                }
                depth--;
            } else if (c == ',' && depth == 0) {
                break;
            }
            start--;
        }
        String def = ddl.substring(start, primaryKeyStart).trim();
        if (def.isEmpty()) {
            return null;
        }
        String first = def.split("\\s+")[0];
        return normalize(first);
    }

    private static void addName(List<String> out, String raw) {
        String n = normalize(raw);
        if (n != null && !n.isEmpty()) {
            out.add(n);
        }
    }

    /** Strip whitespace + double-quotes and lower-case for case-insensitive match. */
    private static String normalize(String raw) {
        if (raw == null) {
            return null;
        }
        String s = raw.trim();
        if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) {
            // Quoted identifiers are case-sensitive in CQL; preserve case but drop quotes.
            return s.substring(1, s.length() - 1);
        }
        return s.isEmpty() ? "" : s.toLowerCase(Locale.ROOT);
    }
}
