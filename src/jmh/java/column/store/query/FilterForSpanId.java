package column.store.query;

import column.store.api.column.Column;
import column.store.api.column.IdColumn;
import column.store.api.query.Query;
import column.store.api.read.Reader;
import column.store.csv.read.CSVReader;
import column.store.parquet.read.ParquetReader;

import static column.store.api.query.Filter.whereId;

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

public class FilterForSpanId {

  // always present
  private static final IdColumn SPAN_ID = Column.forId("span.id-id_64");
  private static final byte[] TARGET_ID = new byte[] { -120, -84, -113, 10, -114, -22, 16, 104 };

  // TODO: proper path
  private static final Path SPANS_PARQUET = Path.of("..", "column-store-tools", "bench14410517177477105447");
  private static final Path SPANS_CSV = Path.of("C:\\workspaces\\column-store-tools\\bench10666860571716979359\\csv15339267480058856055.csv");
  @State(Scope.Thread)
  public static class BenchState {
    public Reader reader;
    public Query query;

    @Setup(Level.Iteration)
    public void setup() {
//      UnaryOperator<org.apache.parquet.hadoop.ParquetReader.Builder<Object>> config = builder ->
//              builder
//                      .useColumnIndexFilter()
//                      .useBloomFilter()
//                      .useDictionaryFilter()
//                      .useRecordFilter();
//
//      reader = new ParquetReader(config);
      reader = new CSVReader();
      query = Query.from(SPANS_CSV)
              .select(SPAN_ID)
              .filter(whereId(SPAN_ID).is(TARGET_ID))
              .allOf();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void read(final BenchState state, final Blackhole blackhole) throws IOException {
    var reader = state.reader;
    var query = state.query;

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
