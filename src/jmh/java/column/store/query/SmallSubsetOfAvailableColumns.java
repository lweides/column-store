package column.store.query;

import column.store.api.column.BooleanColumn;
import column.store.api.column.Column;
import column.store.api.column.IdColumn;
import column.store.api.column.LongColumn;
import column.store.api.column.StringColumn;
import column.store.api.query.Query;
import column.store.api.read.Reader;
import column.store.parquet.read.ParquetReader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.UnaryOperator;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

public class SmallSubsetOfAvailableColumns {

  // always present
  private static final IdColumn SPAN_ID = Column.forId("span.id-id_64");
  private static final StringColumn SPAN_KIND = Column.forString("span.kind-string");
  private static final LongColumn START_TIME = Column.forLong("start_time-long");
  private static final BooleanColumn REQUEST_IS_FAILED = Column.forBoolean("request.is_failed-boolean");

  // not always present
  private static final StringColumn HTTP_METHOD = Column.forString("http.method-string");

  // TODO: proper path
  private static final Path SPANS = Path.of("..", "column-store-tools", "bench14410517177477105447");

  @State(Scope.Thread)
  public static class BenchState {
    public Reader reader;
    public Query query;

    @Setup(Level.Iteration)
    public void setup() {
      UnaryOperator<org.apache.parquet.hadoop.ParquetReader.Builder<Object>> config = UnaryOperator.identity();
      reader = new ParquetReader(config);

      query = Query.from(SPANS)
              .select(SPAN_ID, SPAN_KIND, START_TIME, REQUEST_IS_FAILED, HTTP_METHOD)
              .allOf();
    }
  }

//  @Benchmark
//  @BenchmarkMode(Mode.AverageTime)
  public void read(final BenchState state, final Blackhole blackhole) throws IOException {
    var reader = state.reader;
    var query = state.query;

    reader.query(query);
    var spanIds = reader.of(SPAN_ID);
    var spanKinds = reader.of(SPAN_KIND);
    var startTimes = reader.of(START_TIME);
    var requestsFailed = reader.of(REQUEST_IS_FAILED);
    var httpMethods = reader.of(HTTP_METHOD);

    while (reader.hasNext()) {
      reader.next();
      if (spanIds.isPresent()) {
        blackhole.consume(spanIds.get());
      }
      if (spanKinds.isPresent()) {
        blackhole.consume(spanKinds.get());
      }
      if (startTimes.isPresent()) {
        blackhole.consume(startTimes.get());
      }
      if (requestsFailed.isPresent()) {
        blackhole.consume(requestsFailed.get());
      }
      if (httpMethods.isPresent()) {
        blackhole.consume(httpMethods.get());
      }
    }
  }
}
