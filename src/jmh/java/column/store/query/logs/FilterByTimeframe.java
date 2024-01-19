package column.store.query.logs;

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
import column.store.api.column.LongColumn;
import column.store.api.column.StringColumn;
import column.store.api.query.Query;
import column.store.api.read.Reader;

@SuppressWarnings("checkstyle:MagicNumber")
public class FilterByTimeframe {

  private static final StringColumn PAYLOAD = Column.forString("payload");
  private static final LongColumn TIMESTAMP = Column.forLong("timestamp");

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
      data = Utils.data("logs", readerType, isStable);
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
            .select(TIMESTAMP, PAYLOAD);
    var query = timeframe(builder, 1704366859442000001L, 1704376981960700160L);
    reader.query(query);

    var timestamps = reader.of(TIMESTAMP);
    var payloads = reader.of(PAYLOAD);

    while (reader.hasNext()) {
      reader.next();
      if (timestamps.isPresent()) {
        blackhole.consume(timestamps.get());
      }
      if (payloads.isPresent()) {
        blackhole.consume(payloads.get());
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void smallTimeframe(final BenchState state, final Blackhole blackhole) throws IOException {
    var reader = state.reader;
    var builder = Query.from(state.data)
            .select(TIMESTAMP, PAYLOAD);
    var query = timeframe(builder, 1704366859442000001L, 1704371197664300032L);
    reader.query(query);

    var timestamps = reader.of(TIMESTAMP);
    var payloads = reader.of(PAYLOAD);

    while (reader.hasNext()) {
      reader.next();
      if (timestamps.isPresent()) {
        blackhole.consume(timestamps.get());
      }
      if (payloads.isPresent()) {
        blackhole.consume(payloads.get());
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void timeframeMatchingAllRecords(final BenchState state, final Blackhole blackhole) throws IOException {
    var reader = state.reader;
    var builder = Query.from(state.data)
            .select(TIMESTAMP, PAYLOAD);
    var query = timeframe(builder, 0, Long.MAX_VALUE);
    reader.query(query);

    var timestamps = reader.of(TIMESTAMP);
    var payloads = reader.of(PAYLOAD);

    while (reader.hasNext()) {
      reader.next();
      if (timestamps.isPresent()) {
        blackhole.consume(timestamps.get());
      }
      if (payloads.isPresent()) {
        blackhole.consume(payloads.get());
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void timeframeMatchingNoRecords(final BenchState state, final Blackhole blackhole) throws IOException {
    var reader = state.reader;
    var builder = Query.from(state.data)
            .select(TIMESTAMP, PAYLOAD);
    var query = timeframe(builder, 10, 20);
    reader.query(query);

    var timestamps = reader.of(TIMESTAMP);
    var payloads = reader.of(PAYLOAD);

    while (reader.hasNext()) {
      reader.next();
      if (timestamps.isPresent()) {
        blackhole.consume(timestamps.get());
      }
      if (payloads.isPresent()) {
        blackhole.consume(payloads.get());
      }
    }
  }

  private static Query timeframe(final Query.Builder builder, final long startNanos, final long endNanos) {
    return builder
            .filter(whereLong(TIMESTAMP).isGreaterThan(startNanos))
            .filter(whereLong(TIMESTAMP).isLessThan(endNanos))
            .allOf();
  }
}
