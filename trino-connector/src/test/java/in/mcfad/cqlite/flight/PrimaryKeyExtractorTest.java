package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.PrimaryKeyExtractor.KeyColumn;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link PrimaryKeyExtractor} (issue #944). */
class PrimaryKeyExtractorTest {

    /** Canonical (stored-case) names of a key-column list, for terse assertions. */
    private static List<String> names(List<KeyColumn> cols) {
        return cols.stream().map(KeyColumn::name).toList();
    }

    @Test
    void inlineSingleColumnPrimaryKey() {
        var keys = PrimaryKeyExtractor.extract(
                "CREATE TABLE ks.t (id int PRIMARY KEY, name text, score int)");
        assertEquals(List.of("id"), names(keys.partitionKey()));
        assertTrue(keys.clusteringColumns().isEmpty());
    }

    @Test
    void simpleCompositePrimaryKeyWithClustering() {
        var keys = PrimaryKeyExtractor.extract(
                "CREATE TABLE ks.t (pk int, ck1 text, ck2 int, v text, PRIMARY KEY (pk, ck1, ck2))");
        assertEquals(List.of("pk"), names(keys.partitionKey()));
        assertEquals(List.of("ck1", "ck2"), names(keys.clusteringColumns()));
    }

    @Test
    void compositePartitionKey() {
        var keys = PrimaryKeyExtractor.extract(
                "CREATE TABLE ks.t (a int, b int, c text, v int, PRIMARY KEY ((a, b), c))");
        assertEquals(List.of("a", "b"), names(keys.partitionKey()));
        assertEquals(List.of("c"), names(keys.clusteringColumns()));
    }

    @Test
    void compositePartitionKeyNoClustering() {
        var keys = PrimaryKeyExtractor.extract(
                "CREATE TABLE ks.t (a int, b int, v int, PRIMARY KEY ((a, b)))");
        assertEquals(List.of("a", "b"), names(keys.partitionKey()));
        assertTrue(keys.clusteringColumns().isEmpty());
    }

    @Test
    void quotedIdentifiersPreserveCase() {
        var keys = PrimaryKeyExtractor.extract(
                "CREATE TABLE ks.t (\"Id\" int, \"Ck\" text, PRIMARY KEY (\"Id\", \"Ck\"))");
        assertEquals(List.of("Id"), names(keys.partitionKey()));
        assertEquals(List.of("Ck"), names(keys.clusteringColumns()));
        // The quote flag must be carried so comparison stays case-sensitive.
        assertTrue(keys.partitionKey().get(0).quoted());
        assertTrue(keys.clusteringColumns().get(0).quoted());
    }

    @Test
    void unquotedIdentifierMatchesCaseInsensitively() {
        // CQL folds unquoted identifiers → matches any letter casing.
        KeyColumn unquoted = PrimaryKeyExtractor.extract(
                "CREATE TABLE ks.t (id int PRIMARY KEY)").partitionKey().get(0);
        assertFalse(unquoted.quoted());
        assertTrue(unquoted.matches("id"));
        assertTrue(unquoted.matches("ID"));
        assertTrue(unquoted.matches("Id"));
    }

    @Test
    void quotedIdentifierMatchesOnlyExactCase() {
        // CQL preserves quoted identifiers → only exact-case matches.
        KeyColumn quoted = PrimaryKeyExtractor.extract(
                "CREATE TABLE ks.t (\"Id\" int PRIMARY KEY)").partitionKey().get(0);
        assertTrue(quoted.quoted());
        assertTrue(quoted.matches("Id"));
        assertFalse(quoted.matches("id"), "quoted \"Id\" must not equal unquoted id");
        assertFalse(quoted.matches("ID"));
    }

    @Test
    void allKeyColumnsUnionsPartitionAndClustering() {
        var keys = PrimaryKeyExtractor.extract(
                "CREATE TABLE ks.t (pk int, ck text, v int, PRIMARY KEY (pk, ck))");
        assertTrue(keys.allKeyColumns().containsAll(List.of("pk", "ck")));
        assertEquals(2, keys.allKeyColumns().size());
    }

    @Test
    void unparseableDdlYieldsEmptyKeys() {
        var keys = PrimaryKeyExtractor.extract("not a create table statement");
        assertTrue(keys.partitionKey().isEmpty());
        assertTrue(keys.clusteringColumns().isEmpty());
    }

    @Test
    void nullDdlIsSafe() {
        var keys = PrimaryKeyExtractor.extract(null);
        assertTrue(keys.partitionKey().isEmpty());
        assertTrue(keys.clusteringColumns().isEmpty());
    }
}
