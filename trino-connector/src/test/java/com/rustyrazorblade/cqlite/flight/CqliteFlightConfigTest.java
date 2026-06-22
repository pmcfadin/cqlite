package com.rustyrazorblade.cqlite.flight;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Parsing of the aggregation-pushdown gate properties (issue #893) and existing
 * connector config keys.
 */
class CqliteFlightConfigTest {

    private static Map<String, String> base() {
        Map<String, String> m = new HashMap<>();
        m.put("cqlite.sidecar-uri", "http://cassandra:9043");
        return m;
    }

    @Test
    void defaultsWhenGateUnset() {
        CqliteFlightConfig config = CqliteFlightConfig.fromMap(base());
        assertEquals(GroupByPushdownPolicy.AUTOMATIC, config.groupByPushdown());
        assertEquals(CqliteFlightConfig.DEFAULT_MAX_GROUP_RATIO, config.maxGroupRatio());
        assertEquals(CqliteFlightConfig.DEFAULT_FLIGHT_PORT, config.flightPort());
    }

    @Test
    void parsesPolicyCaseInsensitively() {
        Map<String, String> m = base();
        m.put("cqlite.aggregation-pushdown-group-by", "NeVeR");
        assertEquals(GroupByPushdownPolicy.NEVER, CqliteFlightConfig.fromMap(m).groupByPushdown());
    }

    @Test
    void parsesMaxGroupRatio() {
        Map<String, String> m = base();
        m.put("cqlite.aggregation-pushdown-max-group-ratio", "0.25");
        assertEquals(0.25, CqliteFlightConfig.fromMap(m).maxGroupRatio());
    }

    @Test
    void rejectsInvalidPolicy() {
        Map<String, String> m = base();
        m.put("cqlite.aggregation-pushdown-group-by", "sometimes");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));
    }

    @Test
    void rejectsOutOfRangeRatio() {
        Map<String, String> m = base();
        m.put("cqlite.aggregation-pushdown-max-group-ratio", "1.5");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(m));

        Map<String, String> z = base();
        z.put("cqlite.aggregation-pushdown-max-group-ratio", "0");
        assertThrows(IllegalArgumentException.class, () -> CqliteFlightConfig.fromMap(z));
    }
}
