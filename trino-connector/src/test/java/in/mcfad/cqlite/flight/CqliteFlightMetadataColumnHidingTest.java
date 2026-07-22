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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Per-column degradation (issues #2229, #2815): a column whose type still cannot be
 * mapped is hidden from the Trino schema (owner decision: hide + warn), with the
 * warning emitted on EVERY projection (loud + durable, not once per table), and an
 * all-unsupported table fails with a clear error. Collections of a supported leaf
 * now project (issue #2815) and are NOT hidden.
 */
class CqliteFlightMetadataColumnHidingTest {

    private static Field scalar(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), List.of());
    }

    private static Field list(String name, Field element) {
        return new Field(name, FieldType.nullable(ArrowType.List.INSTANCE), List.of(element));
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
        // A list of a SUPPORTED leaf (text) now projects (issue #2815) — kept, not hidden.
        Field tags = list("tags", scalar("item", ArrowType.Utf8.INSTANCE));
        // A list of an UNSUPPORTED leaf (decimal) is still hidden — attributable to the leaf.
        Field bad = list("bad", scalar("item", new ArrowType.Decimal(38, 9, 128)));
        Schema schema = new Schema(List.of(
                scalar("id", new ArrowType.Int(32, true)),
                decimal,
                scalar("name", ArrowType.Utf8.INSTANCE),
                tags,
                bad,
                scalar("t", new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.NANOSECOND, 64))));

        List<String> hidden = new ArrayList<>();
        SchemaTableName tn = new SchemaTableName("ks", "accounts");
        List<Field> supported = CqliteFlightMetadata.supportedFields(tn, schema, hidden::add);

        // id, name, tags (array), and time survive; decimal + decimal-list are hidden.
        assertEquals(List.of("id", "name", "tags", "t"), names(supported));
        assertEquals(2, hidden.size());
        assertTrue(hidden.stream().anyMatch(h -> h.startsWith("bal ")));
        assertTrue(hidden.stream().anyMatch(h -> h.startsWith("bad ")));
    }

    @Test
    void hiddenColumnWarningIsEmittedOnEveryProjection() {
        // Issue #2815: the once-per-table suppression made the drop invisible after the
        // first DESCRIBE. The hidden set must be reported on BOTH successive projections.
        Field decimal = scalar("bal", new ArrowType.Decimal(38, 9, 128));
        Schema schema = new Schema(List.of(
                scalar("id", new ArrowType.Int(32, true)), decimal));
        SchemaTableName tn = new SchemaTableName("ks", "accounts");

        List<String> first = new ArrayList<>();
        CqliteFlightMetadata.supportedFields(tn, schema, first::add);
        List<String> second = new ArrayList<>();
        CqliteFlightMetadata.supportedFields(tn, schema, second::add);

        assertEquals(1, first.size());
        assertEquals(1, second.size(), "hidden column must be warned on the SECOND projection too");
        assertTrue(second.get(0).startsWith("bal "));
    }

    @Test
    void allSupportedNeverWarns() {
        Schema schema = new Schema(List.of(
                scalar("id", new ArrowType.Int(32, true)),
                scalar("name", ArrowType.Utf8.INSTANCE)));
        List<String> hidden = new ArrayList<>();
        SchemaTableName tn = new SchemaTableName("ks", "plain");
        List<Field> supported = CqliteFlightMetadata.supportedFields(tn, schema, hidden::add);
        assertEquals(List.of("id", "name"), names(supported));
        assertTrue(hidden.isEmpty());
    }

    @Test
    void allUnsupportedFailsWithClearError() {
        Schema schema = new Schema(List.of(
                scalar("bal", new ArrowType.Decimal(38, 9, 128)),
                list("bad", scalar("item", new ArrowType.Decimal(38, 9, 128)))));
        List<String> hidden = new ArrayList<>();
        SchemaTableName tn = new SchemaTableName("ks", "opaque");
        TrinoException ex = assertThrows(TrinoException.class,
                () -> CqliteFlightMetadata.supportedFields(tn, schema, hidden::add));
        assertTrue(ex.getMessage().contains("no Trino-supported columns"));
    }
}
