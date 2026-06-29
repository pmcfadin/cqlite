package in.mcfad.cqlite.flight;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts a single {@code CREATE TABLE} statement for a given table out of the
 * full keyspace schema blob returned by Sidecar's schema endpoint. The
 * cqlite-flight server parses one table's DDL, so the connector isolates it here.
 *
 * <p>Cassandra's schema output emits each statement terminated by {@code ;} and
 * never embeds {@code ;} inside a statement, so we capture from the matching
 * {@code CREATE TABLE} up to the next {@code ;}.
 */
public final class CreateTableExtractor {
    private CreateTableExtractor() {}

    /** Find the {@code CREATE TABLE} for {@code keyspace.table} (case-insensitive). */
    public static Optional<String> extract(String keyspaceSchema, String keyspace, String table) {
        // Optional keyspace qualifier, optional double-quotes around identifiers.
        String ks = Pattern.quote(keyspace);
        String tbl = Pattern.quote(table);
        String regex = "(?is)CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?"
                + "(?:\"?" + ks + "\"?\\s*\\.\\s*)?\"?" + tbl + "\"?\\s*\\(";
        Matcher matcher = Pattern.compile(regex).matcher(keyspaceSchema);
        if (!matcher.find()) {
            return Optional.empty();
        }
        int start = matcher.start();
        // Capture the balanced column-definition parentheses (which include the
        // PRIMARY KEY clause), dropping any trailing WITH ... table options — the
        // server only needs columns and keys, and option syntax can trip the parser.
        int open = matcher.end() - 1; // index of the opening '('
        int depth = 0;
        int close = -1;
        for (int i = open; i < keyspaceSchema.length(); i++) {
            char c = keyspaceSchema.charAt(i);
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
            return Optional.empty();
        }
        return Optional.of(keyspaceSchema.substring(start, close + 1).trim() + ";");
    }
}
