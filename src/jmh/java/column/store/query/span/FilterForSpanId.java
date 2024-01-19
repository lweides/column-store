package column.store.query.span;

import static column.store.api.query.Filter.whereId;

import java.io.IOException;
import java.nio.file.Path;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import column.store.Utils;
import column.store.api.column.Column;
import column.store.api.column.IdColumn;
import column.store.api.query.Query;
import column.store.api.read.Reader;

public class FilterForSpanId {

  private static final IdColumn SPAN_ID = Column.forId("span_id-ID_64");
  private static final byte[] SPAN_ID_PRESENT = new byte[] { -122, 54, 121, 117, 21, -38, -54, 65 };
  private static final byte[] SPAN_ID_NOT_PRESENT = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };

  @State(Scope.Thread)
  public static class BenchState {

    @Param({ "parquet", "csv" })
    private String readerType;
    @Param({ "true", "false" })
    private boolean isStable;
    private Reader reader;
    private Path data;

    @Setup(Level.Trial)
    public void setup() {
      reader = Utils.reader(readerType);
      data = Utils.data("spans", readerType, isStable);
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void spanIdPresent(final BenchState state, final Blackhole blackhole) throws IOException {
    var reader = state.reader;
    var query = Query.from(state.data)
            .select(SPAN_ID)
            .filter(whereId(SPAN_ID).is(SPAN_ID_PRESENT))
            .allOf();

    reader.query(query);
    var spanIds = reader.of(SPAN_ID);

    while (reader.hasNext()) {
      reader.next();
      if (spanIds.isPresent()) {
        blackhole.consume(spanIds.get());
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void spanIdNotPresent(final BenchState state, final Blackhole blackhole) throws IOException {
    var reader = state.reader;
    var query = Query.from(state.data)
            .select(SPAN_ID)
            .filter(whereId(SPAN_ID).is(SPAN_ID_NOT_PRESENT))
            .allOf();

    reader.query(query);
    var spanIds = reader.of(SPAN_ID);

    while (reader.hasNext()) {
      reader.next();
      if (spanIds.isPresent()) {
        blackhole.consume(spanIds.get());
      }
    }
  }
}
