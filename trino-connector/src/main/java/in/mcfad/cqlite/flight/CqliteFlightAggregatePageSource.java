package in.mcfad.cqlite.flight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The finalize split's page source (issue #841): fans out to every token range's
 * pinned replica, pulls each range's PARTIAL aggregate, merges them via
 * {@link PartialAggregateMerger}, and emits the FULLY MERGED result as a single
 * Trino {@link Page} (Trino does not re-aggregate across splits).
 *
 * <p>Each range's DoGet ticket carries that range's token bounds + the pushed
 * filter + the wire {@link AggregationSpec}; the server returns one partial row
 * per group whose Arrow columns are [group_by cols..., one column per aggregate
 * output]. We accumulate, then build output blocks in the order of the requested
 * column handles using the {@link FinalizeAggregationPlan}.
 */
public class CqliteFlightAggregatePageSource implements ConnectorPageSource {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CqliteFlightClient client;
    private final CqliteFlightAggregateSplit split;
    private final List<CqliteFlightColumnHandle> columns;
    private final AggregationSpec spec;
    private final FinalizeAggregationPlan plan;

    private boolean finished;

    public CqliteFlightAggregatePageSource(
            CqliteFlightClient client,
            CqliteFlightAggregateSplit split,
            List<CqliteFlightColumnHandle> columns) {
        this.client = client;
        this.split = split;
        this.columns = columns;
        this.spec = parseSpec(split.aggregationJson());
        this.plan = FinalizeAggregationPlan.fromJson(parse(split.finalizePlanJson()));
    }

    @Override
    public SourcePage getNextSourcePage() {
        if (finished) {
            return null;
        }
        finished = true; // a single materialized page holds the whole merged result

        PartialAggregateMerger merger = new PartialAggregateMerger(spec.aggregates());
        Optional<String> filter = split.filterJson();
        JsonNode filterNode = filter.map(CqliteFlightAggregatePageSource::parse).orElse(null);
        JsonNode aggregationNode = parse(split.aggregationJson());

        // Fan out: one DoGet per range to its pinned replica, accumulating partials.
        for (CqliteFlightSplit range : split.ranges()) {
            byte[] ticket = FlightTicketJson.build(
                    range.keyspace(),
                    range.table(),
                    range.ddl(),
                    Optional.empty(),
                    Optional.of(range.tokenStart()),
                    Optional.of(range.tokenEnd()),
                    range.wraparound(),
                    Optional.empty(), // aggregation defines the output projection
                    List.of(),
                    filterNode,
                    aggregationNode);
            try (CqliteFlightClient.StreamHandle handle =
                    client.openStream(range.host(), range.port(), ticket)) {
                while (handle.stream().next()) {
                    accumulate(handle.stream().getRoot(), merger);
                }
            }
        }

        Page page = buildPage(merger.finish());
        return SourcePage.create(page);
    }

    /** Read each partial row from one Arrow batch into the merger. */
    private void accumulate(VectorSchemaRoot root, PartialAggregateMerger merger) {
        int rows = root.getRowCount();
        List<String> groupBy = spec.groupBy();
        for (int r = 0; r < rows; r++) {
            List<Object> keyValues = new ArrayList<>(groupBy.size());
            for (String gc : groupBy) {
                keyValues.add(rawValue(root.getVector(gc), r));
            }
            Map<String, Object> partials = new HashMap<>();
            for (AggregationSpec.Aggregate a : spec.aggregates()) {
                partials.put(a.output(), rawValue(root.getVector(a.output()), r));
            }
            merger.combine(new PartialAggregateMerger.GroupKey(keyValues), partials);
        }
    }

    /** Build the single output page, one block per requested column. */
    private Page buildPage(List<PartialAggregateMerger.MergedGroup> groups) {
        Map<String, FinalizeAggregationPlan.OutputColumn> byResult = new HashMap<>();
        for (FinalizeAggregationPlan.OutputColumn o : plan.outputs()) {
            byResult.put(o.resultName(), o);
        }

        int rowCount = groups.size();
        Block[] blocks = new Block[columns.size()];
        for (int c = 0; c < columns.size(); c++) {
            CqliteFlightColumnHandle col = columns.get(c);
            Type type = col.type();
            FinalizeAggregationPlan.OutputColumn out = byResult.get(col.name());
            BlockBuilder builder = type.createBlockBuilder(null, rowCount);
            for (PartialAggregateMerger.MergedGroup g : groups) {
                Object value = resultValue(out, g);
                if (value == null) {
                    builder.appendNull();
                } else {
                    ArrowToTrino.writeJavaValue(type, value, builder);
                }
            }
            blocks[c] = builder.build();
        }
        return new Page(rowCount, blocks);
    }

    /** Compute one Trino result column's value for one merged group. */
    private Object resultValue(FinalizeAggregationPlan.OutputColumn out, PartialAggregateMerger.MergedGroup g) {
        if (out == null) {
            return null;
        }
        switch (out.kind()) {
            case GROUP -> {
                int idx = plan.groupBy().indexOf(out.primary());
                return g.key().values().get(idx);
            }
            case DIRECT -> {
                return g.outputs().get(out.primary());
            }
            case AVG -> {
                Object sum = g.outputs().get(out.primary());
                Object count = g.outputs().get(out.secondary());
                long c = (count == null) ? 0L : ((Number) count).longValue();
                if (c == 0L || sum == null) {
                    return null; // avg over zero non-null inputs is null
                }
                return ((Number) sum).doubleValue() / (double) c;
            }
            default -> {
                return null;
            }
        }
    }

    /** Read a raw Java value out of an Arrow vector at row {@code i}, or null. */
    private static Object rawValue(FieldVector vector, int i) {
        if (vector == null || vector.isNull(i)) {
            return null;
        }
        return ArrowToTrino.readJavaValue(vector, i);
    }

    private static AggregationSpec parseSpec(String json) {
        JsonNode node = parse(json);
        List<String> groupBy = new ArrayList<>();
        node.get("group_by").forEach(n -> groupBy.add(n.asText()));
        List<AggregationSpec.Aggregate> aggregates = new ArrayList<>();
        for (JsonNode a : node.get("aggregates")) {
            String column = a.get("column").isNull() ? null : a.get("column").asText();
            aggregates.add(new AggregationSpec.Aggregate(
                    AggregationSpec.Func.valueOf(a.get("func").asText()),
                    column,
                    a.get("output").asText()));
        }
        return new AggregationSpec(groupBy, aggregates);
    }

    private static JsonNode parse(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Invalid aggregation/finalize JSON on split", e);
        }
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public long getMemoryUsage() {
        return 0;
    }

    @Override
    public void close() {
        finished = true;
    }
}
