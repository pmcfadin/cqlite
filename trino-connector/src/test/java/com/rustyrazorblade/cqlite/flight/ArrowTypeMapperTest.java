package com.rustyrazorblade.cqlite.flight;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ArrowTypeMapperTest {

    private static Field scalar(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), List.of());
    }

    @Test
    void mapsScalarTypes() {
        assertEquals(IntegerType.INTEGER,
                ArrowTypeMapper.toTrino(scalar("i", new ArrowType.Int(32, true))));
        assertEquals(BigintType.BIGINT,
                ArrowTypeMapper.toTrino(scalar("l", new ArrowType.Int(64, true))));
        assertEquals(BooleanType.BOOLEAN,
                ArrowTypeMapper.toTrino(scalar("b", ArrowType.Bool.INSTANCE)));
        assertEquals(VarcharType.VARCHAR,
                ArrowTypeMapper.toTrino(scalar("s", ArrowType.Utf8.INSTANCE)));
        assertInstanceOf(io.trino.spi.type.DoubleType.class,
                ArrowTypeMapper.toTrino(scalar("d",
                        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))));
        assertInstanceOf(io.trino.spi.type.RealType.class,
                ArrowTypeMapper.toTrino(scalar("f",
                        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))));
    }

    @Test
    void uuidExtensionMapsToVarchar() {
        Field uuid = new Field(
                "id",
                new FieldType(true, new ArrowType.FixedSizeBinary(16),
                        null, Map.of("ARROW:extension:name", "arrow.uuid")),
                List.of());
        assertEquals(VarcharType.VARCHAR, ArrowTypeMapper.toTrino(uuid));
    }

    @Test
    void complexTypesAreRejectedAtPlanning() {
        // v1 supports scalar columns; collections/decimal must fail clearly at
        // planning rather than crash mid-scan (mapper↔ArrowToTrino stay in lockstep).
        Field child = scalar("item", new ArrowType.Int(32, true));
        Field list = new Field("xs", FieldType.nullable(ArrowType.List.INSTANCE), List.of(child));
        assertThrows(UnsupportedOperationException.class, () -> ArrowTypeMapper.toTrino(list));

        Field decimal = scalar("d", new ArrowType.Decimal(38, 9, 128));
        assertThrows(UnsupportedOperationException.class, () -> ArrowTypeMapper.toTrino(decimal));
    }
}
