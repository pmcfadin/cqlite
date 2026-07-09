package in.mcfad.cqlite.flight;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Per-column degradation (issue #2229): unsupported-type columns are hidden from
 * the Trino schema (owner decision: hide + warn), warned about once per table, and
 * an all-unsupported table fails with a clear error.
 */
class CqliteFlightMetadataColumnHidingTest {

    private static Field scalar(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), List.of());
    }

    private static List<String> names(List<Field> fields) {
        List<String> out = new ArrayList<>();
        for (Field f : fields) {
            out.add(f.getName());
        }
        return out;
    }

    @Test
    void unsupportedColumnsAreHiddenSupportedKeptInOrder() {
        Field decimal = scalar("bal", new ArrowType.Decimal(38, 9, 128));
        Field list = new Field("tags", FieldType.nullable(ArrowType.List.INSTANCE),
                List.of(scalar("item", ArrowType.Utf8.INSTANCE)));
        Schema schema = new Schema(List.of(
                scalar("id", new ArrowType.Int(32, true)),
                decimal,
                scalar("name", ArrowType.Utf8.INSTANCE),
                list,
                scalar("t", new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.NANOSECOND, 64))));

        Set<SchemaTableName> warned = ConcurrentHashMap.newKeySet();
        SchemaTableName tn = new SchemaTableName("ks", "accounts");
        List<Field> supported = CqliteFlightMetadata.supportedFields(tn, schema, warned);

        // id, name, and time survive; decimal + list are hidden. Order preserved.
        assertEquals(List.of("id", "name", "t"), names(supported));
        // The table is warned about exactly once (set membership records it).
        assertTrue(warned.contains(tn));
        // Idempotent: a second call keeps the same result and does not re-warn.
        List<Field> again = CqliteFlightMetadata.supportedFields(tn, schema, warned);
        assertEquals(List.of("id", "name", "t"), names(again));
        assertEquals(1, warned.size());
    }

    @Test
    void allSupportedNeverWarns() {
        Schema schema = new Schema(List.of(
                scalar("id", new ArrowType.Int(32, true)),
                scalar("name", ArrowType.Utf8.INSTANCE)));
        Set<SchemaTableName> warned = ConcurrentHashMap.newKeySet();
        SchemaTableName tn = new SchemaTableName("ks", "plain");
        List<Field> supported = CqliteFlightMetadata.supportedFields(tn, schema, warned);
        assertEquals(List.of("id", "name"), names(supported));
        assertTrue(warned.isEmpty());
    }

    @Test
    void allUnsupportedFailsWithClearError() {
        Schema schema = new Schema(List.of(
                scalar("bal", new ArrowType.Decimal(38, 9, 128)),
                new Field("tags", FieldType.nullable(ArrowType.List.INSTANCE),
                        List.of(scalar("item", ArrowType.Utf8.INSTANCE)))));
        Set<SchemaTableName> warned = ConcurrentHashMap.newKeySet();
        SchemaTableName tn = new SchemaTableName("ks", "opaque");
        TrinoException ex = assertThrows(TrinoException.class,
                () -> CqliteFlightMetadata.supportedFields(tn, schema, warned));
        assertTrue(ex.getMessage().contains("no Trino-supported columns"));
    }
}
