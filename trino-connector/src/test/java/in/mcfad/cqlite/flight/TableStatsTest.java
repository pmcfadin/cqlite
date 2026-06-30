package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies {@link TableStats} decodes the snake_case JSON the Rust cqlite-flight
 * {@code table_stats} action emits, and that {@link TableStats#plus} sums
 * per-node responses (issue #944).
 */
class TableStatsTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void decodesServerSnakeCaseJson() throws Exception {
        // Exactly the shape cqlite_flight::stats::TableStatsResponse serializes.
        String json = "{\"live_rows\":2000,\"partition_count\":10,\"sstable_count\":1}";
        TableStats stats = MAPPER.readValue(json, TableStats.class);
        assertEquals(2000, stats.liveRows());
        assertEquals(10, stats.partitionCount());
        assertEquals(1, stats.sstableCount());
    }

    @Test
    void ignoresUnknownFields() throws Exception {
        String json = "{\"live_rows\":5,\"partition_count\":2,\"sstable_count\":1,\"future\":42}";
        TableStats stats = MAPPER.readValue(json, TableStats.class);
        assertEquals(5, stats.liveRows());
    }

    @Test
    void plusSumsAcrossNodes() {
        TableStats a = new TableStats(100, 10, 1);
        TableStats b = new TableStats(50, 5, 2);
        TableStats sum = a.plus(b);
        assertEquals(150, sum.liveRows());
        assertEquals(15, sum.partitionCount());
        assertEquals(3, sum.sstableCount());
    }

    @Test
    void emptyIsZero() {
        assertEquals(0, TableStats.EMPTY.liveRows());
        assertEquals(0, TableStats.EMPTY.partitionCount());
        assertEquals(0, TableStats.EMPTY.sstableCount());
    }
}
