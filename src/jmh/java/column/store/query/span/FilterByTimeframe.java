package column.store.query.span;

import static column.store.api.query.Filter.whereLong;

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
import column.store.api.column.LongColumn;
import column.store.api.query.Query;
import column.store.api.read.Reader;

@SuppressWarnings("checkstyle:MagicNumber")
public class FilterByTimeframe {

  private static final IdColumn SPAN_ID = Column.forId("span_id-id_64");
  private static final LongColumn START_TIME = Column.forLong("start_time-timestamp");
  private static final LongColumn END_TIME = Column.forLong("end_time-timestamp");

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
  public void largeTimeframe(final BenchState state, final Blackhole blackhole) throws IOException {
    var reader = state.reader;
    var builder = Query.from(state.data)
            .select(START_TIME, END_TIME, SPAN_ID);
    var query = timeframe(builder, 1704357275983219000L, 1704367793166432512L);
    reader.query(query);

    var startTimes = reader.of(START_TIME);
    var endTimes = reader.of(END_TIME);
    var spanIds = reader.of(SPAN_ID);

    while (reader.hasNext()) {
      reader.next();

      if (spanIds.isPresent()) {
        blackhole.consume(spanIds.get());
      }
      if (startTimes.isPresent()) {
        blackhole.consume(startTimes.get());
      }
      if (endTimes.isPresent()) {
        blackhole.consume(endTimes.get());
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void smallTimeframe(final BenchState state, final Blackhole blackhole) throws IOException {
    var reader = state.reader;
    var builder = Query.from(state.data)
            .select(START_TIME, END_TIME, SPAN_ID);
    var query = timeframe(builder, 1704357275983219000L, 1704363849222727424L);
    reader.query(query);

    var startTimes = reader.of(START_TIME);
    var endTimes = reader.of(END_TIME);
    var spanIds = reader.of(SPAN_ID);

    while (reader.hasNext()) {
      reader.next();

      if (spanIds.isPresent()) {
        blackhole.consume(spanIds.get());
      }
      if (startTimes.isPresent()) {
        blackhole.consume(startTimes.get());
      }
      if (endTimes.isPresent()) {
        blackhole.consume(endTimes.get());
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void timeframeMatchingAllRecords(final BenchState state, final Blackhole blackhole) throws IOException {
    var reader = state.reader;
    var builder = Query.from(state.data)
            .select(START_TIME, END_TIME, SPAN_ID);
    var query = timeframe(builder, 0, Long.MAX_VALUE);
    reader.query(query);

    var startTimes = reader.of(START_TIME);
    var endTimes = reader.of(END_TIME);
    var spanIds = reader.of(SPAN_ID);

    while (reader.hasNext()) {
      reader.next();

      if (spanIds.isPresent()) {
        blackhole.consume(spanIds.get());
      }
      if (startTimes.isPresent()) {
        blackhole.consume(startTimes.get());
      }
      if (endTimes.isPresent()) {
        blackhole.consume(endTimes.get());
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void timeframeMatchingNoRecords(final BenchState state, final Blackhole blackhole) throws IOException {
    var reader = state.reader;
    var builder = Query.from(state.data)
            .select(START_TIME, END_TIME, SPAN_ID);
    var query = timeframe(builder, 10, 20);
    reader.query(query);

    var startTimes = reader.of(START_TIME);
    var endTimes = reader.of(END_TIME);
    var spanIds = reader.of(SPAN_ID);

    while (reader.hasNext()) {
      reader.next();

      if (spanIds.isPresent()) {
        blackhole.consume(spanIds.get());
      }
      if (startTimes.isPresent()) {
        blackhole.consume(startTimes.get());
      }
      if (endTimes.isPresent()) {
        blackhole.consume(endTimes.get());
      }
    }
  }

  private static Query timeframe(final Query.Builder builder, final long startNanos, final long endNanos) {
    return builder
            .filter(whereLong(START_TIME).isGreaterThan(startNanos))
            .filter(whereLong(END_TIME).isLessThan(endNanos))
            .allOf();
  }
}
