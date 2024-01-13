package column.store.bench;

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
import column.store.api.write.Writer;
import column.store.csv.write.CSVWriter;
import column.store.inmemory.InMemoryReader;
import column.store.inmemory.InMemoryReader.BooleanByteReader;
import column.store.inmemory.InMemoryReader.DoubleByteReader;
import column.store.inmemory.InMemoryReader.IdByteReader;
import column.store.inmemory.InMemoryReader.LongByteReader;
import column.store.inmemory.InMemoryReader.StringByteReader;
import column.store.parquet.write.ParquetWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public final class SpansBench {

  public static void main(final String[] args) throws IOException {
    var reader = new InMemoryReader();
    var path = Path.of("..", "column-store-tools", "4a5355be-70fa-4a24-926e-28392ad6a5bc");
    var query = Query.from(path).select(Column.forBoolean("dummy")).allOf();
    reader.query(query);

    var columns = reader.columns();
    var byType = columns.stream().collect(Collectors.groupingBy(Column::type));

    var booleanReaders = booleanReaders(byType, reader);
    var doubleReaders = doubleReaders(byType, reader);
    var idReaders = idReaders(byType, reader);
    var longReaders = longReaders(byType, reader);
    var stringReaders = stringReaders(byType, reader);

    var writer = parquetWriter(columns);
//    var writer = csvWriter(columns);
    var booleanWriters = booleanWriters(byType, writer);
    var doubleWriters = doubleWriters(byType, writer);
    var idWriters = idWriters(byType, writer);
    var longWriters = longWriters(byType, writer);
    var stringWriters = stringWriters(byType, writer);

    long startNanos = System.nanoTime();
    while (reader.hasNext()) {
      reader.next();
      booleans(booleanReaders, booleanWriters);
      doubles(doubleReaders, doubleWriters);
      ids(idReaders, idWriters);
      longs(longReaders, longWriters);
      strings(stringReaders, stringWriters);
      writer.next();
    }

    reader.close();
    writer.close();

    long duration = System.nanoTime() - startNanos;
    System.out.println("Took " + duration + " nanos, or " + TimeUnit.NANOSECONDS.toSeconds(duration) + " seconds");
    // TODO more stats
  }

  private static ParquetWriter parquetWriter(final List<Column> columns) throws IOException {
    UnaryOperator<ParquetWriter.BuilderImpl> config = builder -> builder
            .withBloomFilterEnabled(true)
            .enableDictionaryEncoding()
            .withCompressionCodec(CompressionCodecName.SNAPPY);

    var tempDir = Files.createTempDirectory(Path.of("."), "bench");
    var parquetPath = Files.createTempFile(tempDir, "parquet", "");
    Files.deleteIfExists(parquetPath);

    return new ParquetWriter(parquetPath, config, columns.toArray(new Column[0]));
  }

  private static CSVWriter csvWriter(final List<Column> columns) throws IOException {
    var tempDir = Files.createTempDirectory(Path.of("."), "bench");
    var csvPath = Files.createTempFile(tempDir, "csv", ".csv");
    return new CSVWriter(csvPath, columns);
  }

  private static void strings(final StringByteReader[] stringReaders, final StringColumnWriter[] stringWriters) {
    for (int i = 0; i < stringReaders.length; i++) {
      var stringReader = stringReaders[i];
      var stringWriter = stringWriters[i];
      if (stringReader.isPresent()) {
        stringWriter.write(stringReader.get());
      } else {
        stringWriter.writeNull();
      }
    }
  }

  private static void longs(final LongByteReader[] longReaders, final LongColumnWriter[] longWriters) {
    for (int i = 0; i < longReaders.length; i++) {
      var longReader = longReaders[i];
      var longWriter = longWriters[i];
      if (longReader.isPresent()) {
        longWriter.write(longReader.get());
      } else {
        longWriter.writeNull();
      }
    }
  }

  private static void ids(final IdByteReader[] idReaders, final IdColumnWriter[] idWriters) {
    for (int i = 0; i < idReaders.length; i++) {
      var idReader = idReaders[i];
      var idWriter = idWriters[i];
      if (idReader.isPresent()) {
        idWriter.write(idReader.get());
      } else {
        idWriter.writeNull();
      }
    }
  }

  private static void doubles(final DoubleByteReader[] doubleReaders, final DoubleColumnWriter[] doubleWriters) {
    for (int i = 0; i < doubleReaders.length; i++) {
      var doubleReader = doubleReaders[i];
      var doubleWriter = doubleWriters[i];
      if (doubleReader.isPresent()) {
        doubleWriter.write(doubleReader.get());
      } else {
        doubleWriter.writeNull();
      }
    }
  }

  private static void booleans(final BooleanByteReader[] booleanReaders, final BooleanColumnWriter[] booleanWriters) {
    for (int i = 0; i < booleanReaders.length; i++) {
      var booleanReader = booleanReaders[i];
      var booleanWriter = booleanWriters[i];
      if (booleanReader.isPresent()) {
        booleanWriter.write(booleanReader.get());
      } else {
        booleanWriter.writeNull();
      }
    }
  }

  private static StringColumnWriter[] stringWriters(final Map<Column.Type, List<Column>> byType, final Writer writer) {
    return byType.getOrDefault(Column.Type.STRING, List.of()).stream()
            .map(column -> writer.of((StringColumn) column))
            .toArray(StringColumnWriter[]::new);
  }

  private static LongColumnWriter[] longWriters(final Map<Column.Type, List<Column>> byType, final Writer writer) {
    return byType.getOrDefault(Column.Type.LONG, List.of()).stream()
            .map(column -> writer.of((LongColumn) column))
            .toArray(LongColumnWriter[]::new);
  }

  private static IdColumnWriter[] idWriters(final Map<Column.Type, List<Column>> byType, final Writer writer) {
    return byType.getOrDefault(Column.Type.ID, List.of()).stream()
            .map(column -> writer.of((IdColumn) column))
            .toArray(IdColumnWriter[]::new);
  }

  private static DoubleColumnWriter[] doubleWriters(final Map<Column.Type, List<Column>> byType, final Writer writer) {
    return byType.getOrDefault(Column.Type.DOUBLE, List.of()).stream()
            .map(column -> writer.of((DoubleColumn) column))
            .toArray(DoubleColumnWriter[]::new);
  }

  private static BooleanColumnWriter[] booleanWriters(final Map<Column.Type, List<Column>> byType, final Writer writer) {
    return byType.getOrDefault(Column.Type.BOOLEAN, List.of()).stream()
            .map(column -> writer.of((BooleanColumn) column))
            .toArray(BooleanColumnWriter[]::new);
  }

  private static StringByteReader[] stringReaders(final Map<Column.Type, List<Column>> byType, final InMemoryReader reader) {
    return byType.getOrDefault(Column.Type.STRING, List.of()).stream()
            .map(column -> reader.of((StringColumn) column))
            .toArray(StringByteReader[]::new);
  }

  private static LongByteReader[] longReaders(final Map<Column.Type, List<Column>> byType, final InMemoryReader reader) {
    return byType.getOrDefault(Column.Type.LONG, List.of()).stream()
            .map(column -> reader.of((LongColumn) column))
            .toArray(LongByteReader[]::new);
  }

  private static IdByteReader[] idReaders(final Map<Column.Type, List<Column>> byType, final InMemoryReader reader) {
    return byType.getOrDefault(Column.Type.ID, List.of()).stream()
            .map(column -> reader.of((IdColumn) column))
            .toArray(IdByteReader[]::new);
  }

  private static DoubleByteReader[] doubleReaders(final Map<Column.Type, List<Column>> byType, final InMemoryReader reader) {
    return byType.getOrDefault(Column.Type.DOUBLE, List.of()).stream()
            .map(column -> reader.of((DoubleColumn) column))
            .toArray(DoubleByteReader[]::new);
  }

  private static BooleanByteReader[] booleanReaders(final Map<Column.Type, List<Column>> byType, final InMemoryReader reader) {
    return byType.getOrDefault(Column.Type.BOOLEAN, List.of()).stream()
            .map(column -> reader.of((BooleanColumn) column))
            .toArray(BooleanByteReader[]::new);
  }

  private SpansBench() {
    // hidden util constructor
  }
}
