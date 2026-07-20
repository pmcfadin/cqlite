package in.mcfad.cqlite.flight;

import in.mcfad.cqlite.flight.PrimaryKeyExtractor.KeyColumn;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalInt;

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

    // ── partitionKeyArity: the independent quote-aware cross-check for split pruning (#2679) ──

    /**
     * The arity scan is the fail-safe guard for composite-PK split pruning: it must NEVER
     * under-count the partition-key components, across every realistic DDL spelling. The caller
     * prunes only when this arity equals {@link PrimaryKeyExtractor#extract}'s partition-key size,
     * so a spelling the main extractor mis-parses is caught here and falls back to full fan-out.
     */
    private static int arity(String ddl) {
        OptionalInt a = PrimaryKeyExtractor.partitionKeyArity(ddl);
        assertTrue(a.isPresent(), "arity should resolve for a locatable PRIMARY KEY: " + ddl);
        return a.getAsInt();
    }

    @Test
    void aritySimpleSingleColumnForms() {
        assertEquals(1, arity("CREATE TABLE ks.t (id int PRIMARY KEY, v text)"));
        assertEquals(1, arity("CREATE TABLE ks.t (pk int, ck text, v text, PRIMARY KEY (pk, ck))"));
        assertEquals(1, arity("CREATE TABLE ks.t (a int, b int, c int, PRIMARY KEY (a, b, c))"));
    }

    @Test
    void arityCompositeForms() {
        assertEquals(2, arity("CREATE TABLE ks.t (a int, b int, v int, PRIMARY KEY ((a, b), c))"));
        assertEquals(2, arity("CREATE TABLE ks.t (a int, b int, v int, PRIMARY KEY ((a, b)))"));
        assertEquals(3, arity("CREATE TABLE ks.t (a int, b int, c int, PRIMARY KEY ((a, b, c)))"));
        // one clustering / multiple clustering do not change the partition-key arity.
        assertEquals(2, arity(
                "CREATE TABLE ks.t (a int, b int, c1 int, c2 int, PRIMARY KEY ((a, b), c1, c2))"));
    }

    @Test
    void arityHandlesWhitespaceNewlinesAndExtraParens() {
        assertEquals(2, arity("CREATE TABLE ks.t (a int, b int,\n  PRIMARY KEY (  ( a ,  b ) , c ))"));
    }

    @Test
    void arityIgnoresTrailingWithClauses() {
        assertEquals(2, arity(
                "CREATE TABLE ks.t (a int, b int, c int, PRIMARY KEY ((a, b), c)) "
                        + "WITH CLUSTERING ORDER BY (c DESC)"));
        assertEquals(2, arity(
                "CREATE TABLE ks.t (a int, b int, PRIMARY KEY ((a, b))) "
                        + "WITH compaction = {'class':'STCS'}"));
    }

    /**
     * Quoted identifiers containing structural characters are the catastrophic under/over-count
     * cases for the main extractor. The arity scan masks quoted contents first, so it stays
     * correct — and the pruning guard compares the two, so a mismatch (see below) blocks pruning.
     */
    @Test
    void arityCorrectForQuotedIdentifiersWithSpecialChars() {
        // ')' inside a quoted identifier: the main extractor UNDER-counts to 1 (row-loss risk),
        // the arity scan correctly reports 2.
        assertEquals(2, arity("CREATE TABLE ks.t (\"a)b\" int, c int, PRIMARY KEY ((\"a)b\", c)))"));
        // ',' inside a quoted identifier: the main extractor OVER-counts to 3, arity reports 2.
        assertEquals(2, arity("CREATE TABLE ks.t (\"a,b\" int, c int, PRIMARY KEY ((\"a,b\", c)))"));
        // '(' inside a quoted identifier.
        assertEquals(2, arity("CREATE TABLE ks.t (\"a(b\" int, c int, PRIMARY KEY ((\"a(b\", c)))"));
        // A quoted identifier that is itself a keyword phrase.
        assertEquals(2, arity(
                "CREATE TABLE ks.t (\"PRIMARY KEY\" int, c int, PRIMARY KEY ((\"PRIMARY KEY\", c)))"));
        // Mixed-case quoted composite.
        assertEquals(2, arity("CREATE TABLE ks.t (\"Aa\" int, \"Bb\" int, PRIMARY KEY ((\"Aa\", \"Bb\")))"));
    }

    @Test
    void arityEmptyWhenNoPrimaryKeyLocatable() {
        assertTrue(PrimaryKeyExtractor.partitionKeyArity("not a create table").isEmpty());
        assertTrue(PrimaryKeyExtractor.partitionKeyArity(null).isEmpty());
    }

    /**
     * Regression proof of the guard's premise: on the catastrophic quoted-')' composite spelling
     * the MAIN extractor under-counts (1) but the arity scan is correct (2). The split-pruning
     * guard compares these and, on disagreement, refuses to prune — so the under-count can never
     * drive a mis-pruned (row-dropping) plan.
     */
    @Test
    void extractorUnderCountsQuotedParenCompositeButArityDoesNot() {
        String ddl = "CREATE TABLE ks.t (\"a)b\" int, c int, PRIMARY KEY ((\"a)b\", c)))";
        assertEquals(1, PrimaryKeyExtractor.extract(ddl).partitionKey().size(),
                "documents the main-extractor under-count the guard must catch");
        assertEquals(2, arity(ddl), "the independent arity scan is not fooled");
    }
}
