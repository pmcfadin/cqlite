package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies {@link TableStats} decodes the snake_case JSON the Rust cqlite-flight
 * {@code table_stats} action emits, and that {@link TableStats#plus} sums
 * per-node responses (issue #944).
 */
class TableStatsTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void decodesServerSnakeCaseJson() throws Exception {
        // Exactly the shape cqlite_flight::stats::TableStatsResponse serializes,
        // including the completeness flag and skipped count.
        String json = "{\"live_rows\":2000,\"partition_count\":10,\"sstable_count\":1,"
                + "\"complete\":true,\"skipped_sstables\":0}";
        TableStats stats = MAPPER.readValue(json, TableStats.class);
        assertEquals(2000, stats.liveRows());
        assertEquals(10, stats.partitionCount());
        assertEquals(1, stats.sstableCount());
        assertTrue(stats.complete());
        assertEquals(0, stats.skippedSstables());
    }

    @Test
    void decodesIncompleteServerJson() throws Exception {
        String json = "{\"live_rows\":2000,\"partition_count\":10,\"sstable_count\":1,"
                + "\"complete\":false,\"skipped_sstables\":2}";
        TableStats stats = MAPPER.readValue(json, TableStats.class);
        assertFalse(stats.complete());
        assertEquals(2, stats.skippedSstables());
    }

    @Test
    void completeDefaultsFalseWhenFlagAbsent() throws Exception {
        // A response from an older server that predates the `complete` field must
        // decode as INCOMPLETE (boolean default false → fail closed), never
        // spuriously complete.
        String json = "{\"live_rows\":5,\"partition_count\":2,\"sstable_count\":1}";
        TableStats stats = MAPPER.readValue(json, TableStats.class);
        assertEquals(5, stats.liveRows());
        assertFalse(stats.complete(), "absent complete flag must default to false (fail closed)");
        assertEquals(0, stats.skippedSstables());
    }

    @Test
    void ignoresUnknownFields() throws Exception {
        String json = "{\"live_rows\":5,\"partition_count\":2,\"sstable_count\":1,"
                + "\"complete\":true,\"future\":42}";
        TableStats stats = MAPPER.readValue(json, TableStats.class);
        assertEquals(5, stats.liveRows());
    }

    @Test
    void plusSumsAcrossNodes() {
        TableStats a = new TableStats(100, 10, 1, true, 0);
        TableStats b = new TableStats(50, 5, 2, true, 0);
        TableStats sum = a.plus(b);
        assertEquals(150, sum.liveRows());
        assertEquals(15, sum.partitionCount());
        assertEquals(3, sum.sstableCount());
        assertTrue(sum.complete(), "both nodes complete → aggregate complete");
        assertEquals(0, sum.skippedSstables());
    }

    @Test
    void plusIsIncompleteWhenAnyNodeIsIncomplete() {
        // A single node with an undecodable Statistics.db taints the cross-ring
        // total: complete is the logical AND, skipped counts accumulate.
        TableStats complete = new TableStats(100, 10, 1, true, 0);
        TableStats incomplete = new TableStats(50, 5, 2, false, 3);
        TableStats sum = complete.plus(incomplete);
        assertFalse(sum.complete(), "one incomplete node → incomplete aggregate");
        assertEquals(3, sum.skippedSstables());

        // EMPTY (the aggregate seed) is complete and must not taint a real node.
        assertTrue(TableStats.EMPTY.plus(complete).complete());
    }

    @Test
    void emptyIsZeroAndComplete() {
        assertEquals(0, TableStats.EMPTY.liveRows());
        assertEquals(0, TableStats.EMPTY.partitionCount());
        assertEquals(0, TableStats.EMPTY.sstableCount());
        assertTrue(TableStats.EMPTY.complete(), "no SSTables → trivially complete");
        assertEquals(0, TableStats.EMPTY.skippedSstables());
    }
}
