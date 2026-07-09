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
 *
 * <p>Replica failover (issue #2241): each range's fan-out DoGet uses the shared {@link
 * ReplicaFailoverStream} over that range's ordered {@link CqliteFlightSplit#replicaHosts()}, so
 * a down primary fails over to the next replica owning the range before any of ITS partial rows
 * are consumed. A range whose every replica is unreachable propagates loudly — an aggregate
 * partial is never silently dropped.
 */
public class CqliteFlightAggregatePageSource implements ConnectorPageSource {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ReplicaFailoverStream.StreamOpener opener;
    private final CqliteFlightAggregateSplit split;
    private final List<CqliteFlightColumnHandle> columns;
    private final AggregationSpec spec;
    private final FinalizeAggregationPlan plan;

    private boolean finished;

    public CqliteFlightAggregatePageSource(
            CqliteFlightClient client,
            CqliteFlightAggregateSplit split,
            List<CqliteFlightColumnHandle> columns) {
        this(ReplicaFailoverStream.adapt(client), split, columns);
    }

    /** Package-private seam: inject the opener directly for unit tests (issue #2241). */
    CqliteFlightAggregatePageSource(
            ReplicaFailoverStream.StreamOpener opener,
            CqliteFlightAggregateSplit split,
            List<CqliteFlightColumnHandle> columns) {
        this.opener = opener;
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

        // Fan out: one DoGet per range, with availability failover across that range's ordered
        // replica list (issue #2241) — a range's partial is never silently dropped: an
        // unreachable range propagates loudly instead of being skipped.
        for (CqliteFlightSplit range : split.ranges()) {
            byte[] ticket = buildRangeTicket(range, filterNode, aggregationNode);
            try (ReplicaFailoverStream stream =
                    new ReplicaFailoverStream(range.replicaHosts(), range.port(), ticket, opener)) {
                while (stream.next()) {
                    accumulate(stream.getRoot(), spec, merger);
                }
            }
        }

        Page page = buildPage(merger.finish());
        return SourcePage.create(page);
    }

    /**
     * Build the DoGet ticket for one token range's finalize fan-out. Package-private
     * (not private) so {@code CqliteFlightAggregatePageSourceTest} exercises this exact
     * production code path — it must carry the range's {@link CqliteFlightSplit#snapshot()}
     * through to the ticket's {@code snapshot} field: present in {@link ReadMode#SNAPSHOT},
     * {@link Optional#empty()} in {@link ReadMode#LIVE} (issue #2105).
     */
    static byte[] buildRangeTicket(CqliteFlightSplit range, JsonNode filterNode, JsonNode aggregationNode) {
        return FlightTicketJson.build(
                range.keyspace(),
                range.table(),
                range.ddl(),
                range.snapshot(), // snapshot mode names it; live mode = empty (#2105)
                Optional.of(range.tokenStart()),
                Optional.of(range.tokenEnd()),
                range.wraparound(),
                Optional.empty(), // aggregation defines the output projection
                List.of(),
                filterNode,
                aggregationNode);
    }

    /**
     * Read each partial row from one Arrow batch into the merger. Package-private and
     * static so {@code CqliteFlightAggregatePageSourceTest} exercises this exact
     * production path with a real {@link VectorSchemaRoot} + {@link AggregationSpec}.
     */
    static void accumulate(VectorSchemaRoot root, AggregationSpec spec, PartialAggregateMerger merger) {
        int rows = root.getRowCount();
        List<String> groupBy = spec.groupBy();
        List<AggregationSpec.Aggregate> aggregates = spec.aggregates();

        // Resolve every projected vector ONCE, up front (issue #2262), via the shared
        // missing-column guard (issue #2273) — a missing vector is a hard error naming
        // the column, NOT a silently null group key / partial-aggregate value that would
        // corrupt the GROUP BY result set undetected. See ArrowToTrino#requireVector for
        // the full schema-drift + error-code rationale (shared with ArrowToTrino#toPage).
        List<FieldVector> groupVectors = new ArrayList<>(groupBy.size());
        for (String gc : groupBy) {
            groupVectors.add(ArrowToTrino.requireVector(root, gc, "Aggregate group-by column"));
        }
        List<FieldVector> aggregateVectors = new ArrayList<>(aggregates.size());
        for (AggregationSpec.Aggregate a : aggregates) {
            aggregateVectors.add(ArrowToTrino.requireVector(root, a.output(), "Aggregate output column"));
        }

        for (int r = 0; r < rows; r++) {
            List<Object> keyValues = new ArrayList<>(groupBy.size());
            for (FieldVector v : groupVectors) {
                keyValues.add(rawValue(v, r));
            }
            Map<String, Object> partials = new HashMap<>();
            for (int i = 0; i < aggregates.size(); i++) {
                partials.put(aggregates.get(i).output(), rawValue(aggregateVectors.get(i), r));
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
        // Callers (accumulate via requireVector) guarantee vector != null — an absent
        // projected vector is rejected upstream (issue #2262). A null CELL within the
        // present vector is normal and still yields null here (no regression to #2238).
        if (vector.isNull(i)) {
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
