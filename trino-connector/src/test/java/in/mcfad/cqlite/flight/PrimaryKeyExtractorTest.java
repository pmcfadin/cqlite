package in.mcfad.cqlite.flight;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link PrimaryKeyExtractor} (issue #944). */
class PrimaryKeyExtractorTest {

    @Test
    void inlineSingleColumnPrimaryKey() {
        var keys = PrimaryKeyExtractor.extract(
                "CREATE TABLE ks.t (id int PRIMARY KEY, name text, score int)");
        assertEquals(List.of("id"), keys.partitionKey());
        assertTrue(keys.clusteringColumns().isEmpty());
    }

    @Test
    void simpleCompositePrimaryKeyWithClustering() {
        var keys = PrimaryKeyExtractor.extract(
                "CREATE TABLE ks.t (pk int, ck1 text, ck2 int, v text, PRIMARY KEY (pk, ck1, ck2))");
        assertEquals(List.of("pk"), keys.partitionKey());
        assertEquals(List.of("ck1", "ck2"), keys.clusteringColumns());
    }

    @Test
    void compositePartitionKey() {
        var keys = PrimaryKeyExtractor.extract(
                "CREATE TABLE ks.t (a int, b int, c text, v int, PRIMARY KEY ((a, b), c))");
        assertEquals(List.of("a", "b"), keys.partitionKey());
        assertEquals(List.of("c"), keys.clusteringColumns());
    }

    @Test
    void compositePartitionKeyNoClustering() {
        var keys = PrimaryKeyExtractor.extract(
                "CREATE TABLE ks.t (a int, b int, v int, PRIMARY KEY ((a, b)))");
        assertEquals(List.of("a", "b"), keys.partitionKey());
        assertTrue(keys.clusteringColumns().isEmpty());
    }

    @Test
    void quotedIdentifiersPreserveCase() {
        var keys = PrimaryKeyExtractor.extract(
                "CREATE TABLE ks.t (\"Id\" int, \"Ck\" text, PRIMARY KEY (\"Id\", \"Ck\"))");
        assertEquals(List.of("Id"), keys.partitionKey());
        assertEquals(List.of("Ck"), keys.clusteringColumns());
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
