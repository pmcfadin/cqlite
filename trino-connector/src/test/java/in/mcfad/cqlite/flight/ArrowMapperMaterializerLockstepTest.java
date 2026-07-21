package in.mcfad.cqlite.flight;

import io.trino.spi.Page;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Lockstep guard (issue #2815, #2679-class trap): the set of Arrow shapes
 * {@link ArrowTypeMapper} advertises MUST equal the set {@link ArrowToTrino} can
 * materialize. This test builds one batch carrying a live vector for EVERY Arrow
 * shape the mapper accepts — including the complex List/Struct/Map added in #2815 —
 * resolves each column's Trino type via the real {@link ArrowTypeMapper#toTrino},
 * then drives {@link ArrowToTrino#toPage}. If the mapper ever accepts a shape the
 * materializer cannot build (or vice versa) this fails with the offending
 * {@code UnsupportedOperationException} rather than surfacing at scan time.
 *
 * <p>Complements {@link ArrowToTrinoGoldenTest} (server-emitted scalar drift) with
 * an explicit complex-type lockstep assertion the hand-built scalar tests lack.
 */
class ArrowMapperMaterializerLockstepTest {

    @Test
    void everyMapperAcceptedShapeMaterializes() {
        try (BufferAllocator allocator = new RootAllocator()) {
            List<FieldVector> vectors = new ArrayList<>();

            // --- Scalars (one representative per accepted Arrow leaf). ----------
            BitVector b = new BitVector("b", allocator);
            b.allocateNew(1); b.set(0, 1); vectors.add(b);
            TinyIntVector i8 = new TinyIntVector("i8", allocator);
            i8.allocateNew(1); i8.set(0, 1); vectors.add(i8);
            SmallIntVector i16 = new SmallIntVector("i16", allocator);
            i16.allocateNew(1); i16.set(0, 1); vectors.add(i16);
            IntVector i32 = new IntVector("i32", allocator);
            i32.allocateNew(1); i32.set(0, 1); vectors.add(i32);
            BigIntVector i64 = new BigIntVector("i64", allocator);
            i64.allocateNew(1); i64.set(0, 1L); vectors.add(i64);
            Float4Vector f4 = new Float4Vector("f4", allocator);
            f4.allocateNew(1); f4.set(0, 1.0f); vectors.add(f4);
            Float8Vector f8 = new Float8Vector("f8", allocator);
            f8.allocateNew(1); f8.set(0, 1.0); vectors.add(f8);
            VarCharVector s = new VarCharVector("s", allocator);
            s.allocateNew(); s.setSafe(0, "x".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            s.setValueCount(1); vectors.add(s);
            VarBinaryVector bin = new VarBinaryVector("bin", allocator);
            bin.allocateNew(); bin.setSafe(0, new byte[] {1}); bin.setValueCount(1); vectors.add(bin);
            DateDayVector date = new DateDayVector("date", allocator);
            date.allocateNew(1); date.set(0, 19_000); vectors.add(date);
            TimeNanoVector time = new TimeNanoVector("time", allocator);
            time.allocateNew(1); time.set(0, 123L); vectors.add(time);
            TimeStampMilliTZVector ts = new TimeStampMilliTZVector("ts", allocator, "UTC");
            ts.allocateNew(1); ts.set(0, 1_700_000_000_000L); vectors.add(ts);

            // --- Complex (issue #2815): List, Struct, Map. ---------------------
            ListVector xs = ListVector.empty("xs", allocator);
            UnionListWriter lw = xs.getWriter();
            lw.setPosition(0); lw.startList(); lw.writeVarChar("a"); lw.endList();
            xs.setValueCount(1); vectors.add(xs);

            StructVector st = StructVector.empty("st", allocator);
            NullableStructWriter sw = st.getWriter();
            sw.setPosition(0); sw.start();
            sw.varChar("k").writeVarChar("v"); sw.integer("n").writeInt(7);
            sw.end(); st.setValueCount(1); vectors.add(st);

            MapVector mp = MapVector.empty("mp", allocator, false);
            UnionMapWriter mw = mp.getWriter();
            mw.setPosition(0); mw.startMap(); mw.startEntry();
            mw.key().varChar().writeVarChar("k"); mw.value().integer().writeInt(1);
            mw.endEntry(); mw.endMap(); mp.setValueCount(1); vectors.add(mp);

            VectorSchemaRoot root = new VectorSchemaRoot(vectors);
            root.setRowCount(1);

            // Resolve each column via the REAL mapper (accepts → included).
            List<CqliteFlightColumnHandle> columns = new ArrayList<>();
            for (FieldVector v : vectors) {
                Type trinoType = ArrowTypeMapper.toTrino(v.getField());
                columns.add(new CqliteFlightColumnHandle(v.getName(), trinoType));
            }

            // The lockstep invariant: every advertised type materializes.
            Page page = assertDoesNotThrow(() -> ArrowToTrino.toPage(root, columns),
                    "a type the mapper accepts must materialize (lockstep, #2815/#2679)");
            assertEquals(vectors.size(), page.getChannelCount());
            assertEquals(1, page.getPositionCount());

            root.close();
            for (FieldVector v : vectors) {
                v.close();
            }
        }
    }
}
