package column.store.spans;

import column.store.api.column.BooleanColumn;
import column.store.api.column.Column;
import column.store.api.column.DoubleColumn;
import column.store.api.column.IdColumn;
import column.store.api.column.LongColumn;
import column.store.api.column.StringColumn;
import column.store.api.query.Query;
import column.store.api.write.BooleanColumnWriter;
import column.store.api.write.DoubleColumnWriter;
import column.store.api.write.IdColumnWriter;
import column.store.api.write.LongColumnWriter;
import column.store.api.write.StringColumnWriter;
import column.store.inmemory.InMemoryReader;
import column.store.parquet.write.ParquetWriter;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class Spans {

  @SuppressWarnings("checkstyle:VisibilityModifier")
  @State(Scope.Thread)
  public static class BenchState {

    public InMemoryReader reader;
    public InMemoryReader.BooleanByteReader[] booleanReaders;
    public InMemoryReader.DoubleByteReader[] doubleReaders;
    public InMemoryReader.IdByteReader[] idReaders;
    public InMemoryReader.LongByteReader[] longReaders;
    public InMemoryReader.StringByteReader[] stringReaders;
    public ParquetWriter writer;
    public BooleanColumnWriter[] booleanWriters;
    public DoubleColumnWriter[] doubleWriters;
    public IdColumnWriter[] idWriters;
    public LongColumnWriter[] longWriters;
    public StringColumnWriter[] stringWriters;

    @Setup(Level.Iteration)
    public void setup() throws IOException {
      reader = new InMemoryReader();
      var path = Path.of("..", "column-store-tools", "4a5355be-70fa-4a24-926e-28392ad6a5bc");
      var query = Query.from(path).select(Column.forBoolean("dummy")).allOf();
      reader.query(query);

      var columns = reader.columns();
      var byType = columns.stream().collect(Collectors.groupingBy(Column::type));

      booleanReaders = byType.getOrDefault(Column.Type.BOOLEAN, List.of()).stream()
              .map(column -> reader.of((BooleanColumn) column))
              .toArray(InMemoryReader.BooleanByteReader[]::new);
      doubleReaders = byType.getOrDefault(Column.Type.DOUBLE, List.of()).stream()
              .map(column -> reader.of((DoubleColumn) column))
              .toArray(InMemoryReader.DoubleByteReader[]::new);
      idReaders = byType.getOrDefault(Column.Type.ID, List.of()).stream()
              .map(column -> reader.of((IdColumn) column))
              .toArray(InMemoryReader.IdByteReader[]::new);
      longReaders = byType.getOrDefault(Column.Type.LONG, List.of()).stream()
              .map(column -> reader.of((LongColumn) column))
              .toArray(InMemoryReader.LongByteReader[]::new);
      stringReaders = byType.getOrDefault(Column.Type.STRING, List.of()).stream()
              .map(column -> reader.of((StringColumn) column))
              .toArray(InMemoryReader.StringByteReader[]::new);

      UnaryOperator<ParquetWriter.BuilderImpl> config = builder -> builder
                      .withBloomFilterEnabled(true)
                      .enableDictionaryEncoding()
                      .withCompressionCodec(CompressionCodecName.SNAPPY);

      var tempDir = Files.createTempDirectory(Path.of("."), "bench");
      var parquetPath = Files.createTempFile(tempDir, "parquet", "");
      Files.deleteIfExists(parquetPath);
      writer = new ParquetWriter(parquetPath, config, columns.toArray(new Column[0]));
      booleanWriters = byType.getOrDefault(Column.Type.BOOLEAN, List.of()).stream()
              .map(column -> writer.of((BooleanColumn) column))
              .toArray(BooleanColumnWriter[]::new);
      doubleWriters = byType.getOrDefault(Column.Type.DOUBLE, List.of()).stream()
              .map(column -> writer.of((DoubleColumn) column))
              .toArray(DoubleColumnWriter[]::new);
      idWriters = byType.getOrDefault(Column.Type.ID, List.of()).stream()
              .map(column -> writer.of((IdColumn) column))
              .toArray(IdColumnWriter[]::new);
      longWriters = byType.getOrDefault(Column.Type.LONG, List.of()).stream()
              .map(column -> writer.of((LongColumn) column))
              .toArray(LongColumnWriter[]::new);
      stringWriters = byType.getOrDefault(Column.Type.STRING, List.of()).stream()
              .map(column -> writer.of((StringColumn) column))
              .toArray(StringColumnWriter[]::new);
    }
  }

//  @Benchmark
//  @BenchmarkMode(Mode.AverageTime)
  public void readRecords(final BenchState state) throws IOException {
    state.reader.reset();
    while (state.reader.hasNext()) {
      state.reader.next();
      for (int i = 0; i < state.booleanReaders.length; i++) {
        var reader = state.booleanReaders[i];
        var writer = state.booleanWriters[i];
        if (reader.isPresent()) {
          writer.write(reader.get());
        } else {
          writer.writeNull();
        }
      }
      for (int i = 0; i < state.doubleReaders.length; i++) {
        var reader = state.doubleReaders[i];
        var writer = state.doubleWriters[i];
        if (reader.isPresent()) {
          writer.write(reader.get());
        } else {
          writer.writeNull();
        }
      }
      for (int i = 0; i < state.idReaders.length; i++) {
        var reader = state.idReaders[i];
        var writer = state.idWriters[i];
        if (reader.isPresent()) {
          writer.write(reader.get());
        } else {
          writer.writeNull();
        }
      }
      for (int i = 0; i < state.longReaders.length; i++) {
        var reader = state.longReaders[i];
        var writer = state.longWriters[i];
        if (reader.isPresent()) {
          writer.write(reader.get());
        } else {
          writer.writeNull();
        }
      }
      for (int i = 0; i < state.stringReaders.length; i++) {
        var reader = state.stringReaders[i];
        var writer = state.stringWriters[i];
        if (reader.isPresent()) {
          writer.write(reader.get());
        } else {
          writer.writeNull();
        }
      }
      state.writer.next();
    }
  }
}
