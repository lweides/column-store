package column.store.query.span;

import column.store.Utils;
import column.store.api.column.Column;
import column.store.api.column.IdColumn;
import column.store.api.query.Query;
import column.store.api.read.Reader;
import static column.store.api.query.Filter.whereId;
import static column.store.util.Conditions.checkState;

import java.io.IOException;
import java.nio.file.Path;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Measurement(iterations = 3)
@Warmup(iterations = 2)
@Fork(1)
public class FilterForTraceId {

  private static final IdColumn TRACE_ID = Column.forId("trace_id-id_128");
  private static final byte[] TRACE_ID_PRESENT = new byte[] { -87, -14, 73, -89, 51, -8, 24, 19, 65, -69, 27, 1, 28, -73, 18, -110 };
  private static final byte[] TRACE_ID_NOT_PRESENT = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

  @State(Scope.Thread)
  public static class BenchState {
    @Param({ "parquet", "csv" })
    private String readerType;
    //    @Param({ "true", "false" })
    private boolean isStable = true;

    private Reader reader;
    private Path data;

    @Setup(Level.Trial)
    public void setup() {
      reader = Utils.reader(readerType);
      data = Utils.data("span", readerType, isStable);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void traceIdPresent(final BenchState state, final Blackhole blackhole) throws IOException {
    var reader = state.reader;
    var query = Query.from(state.data)
            .select(TRACE_ID)
            .filter(whereId(TRACE_ID).is(TRACE_ID_PRESENT))
            .allOf();

    reader.query(query);
    var traceIds = reader.of(TRACE_ID);

    int counter = 0;
    while (reader.hasNext()) {
      reader.next();
      if (traceIds.isPresent()) {
        blackhole.consume(traceIds.get());
        counter++;
      }
    }

    checkState(counter > 0, "Should have more than 0 matches");
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void traceIdNotPresent(final BenchState state, final Blackhole blackhole) throws IOException {
    var reader = state.reader;
    var query = Query.from(state.data)
            .select(TRACE_ID)
            .filter(whereId(TRACE_ID).is(TRACE_ID_NOT_PRESENT))
            .allOf();

    reader.query(query);
    var traceIds = reader.of(TRACE_ID);

    int counter = 0;
    while (reader.hasNext()) {
      reader.next();
      if (traceIds.isPresent()) {
        blackhole.consume(traceIds.get());
        counter++;
      }
    }

    checkState(counter == 0, "Should not have any matches");
  }
}
