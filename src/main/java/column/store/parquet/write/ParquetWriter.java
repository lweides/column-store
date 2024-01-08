package column.store.parquet.write;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.UnaryOperator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.hadoop.ParquetWriter.Builder;

import column.store.api.column.BooleanColumn;
import column.store.api.column.Column;
import column.store.api.column.DoubleColumn;
import column.store.api.column.IdColumn;
import column.store.api.column.LongColumn;
import column.store.api.column.NoSuchColumnException;
import column.store.api.column.StringColumn;
import column.store.api.write.BooleanColumnWriter;
import column.store.api.write.DoubleColumnWriter;
import column.store.api.write.IdColumnWriter;
import column.store.api.write.LongColumnWriter;
import column.store.api.write.StringColumnWriter;
import column.store.api.write.Writer;

import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem;

public class ParquetWriter implements Writer  {

  private final org.apache.parquet.hadoop.ParquetWriter<Object> writer;
  private final BaseWriter[] columnWriters;

  public ParquetWriter(final Path path, final UnaryOperator<BuilderImpl> config, final Column... columns) throws IOException {
    this.columnWriters = new BaseWriter[columns.length];
    fillColumnWriters(columns);
    this.writer = config.apply(new BuilderImpl(path, columnWriters)).build();
  }

  public ParquetWriter(final Path path, final Column... columns) throws IOException {
    this(path, UnaryOperator.identity(), columns);
  }

  private void fillColumnWriters(final Column[] columns) {
    for (int i = 0; i < columns.length; i++) {
      var column = columns[i];
      this.columnWriters[i] = switch (column) {
        case BooleanColumn booleanColumn -> new BooleanWriter(booleanColumn, i);
        case DoubleColumn doubleColumn -> new DoubleWriter(doubleColumn, i);
        case IdColumn idColumn -> new IdWriter(idColumn, i);
        case LongColumn longColumn -> new LongWriter(longColumn, i);
        case StringColumn stringColumn -> new StringWriter(stringColumn, i);
      };
    }
  }

  @Override
  public BooleanColumnWriter of(final BooleanColumn column) {
    return Arrays.stream(columnWriters)
            .filter(columnWriter -> columnWriter.column().equals(column))
            .findFirst()
            .map(columnWriter -> (BooleanColumnWriter) columnWriter)
            .orElseThrow(() -> new NoSuchColumnException(column));
  }

  @Override
  public DoubleColumnWriter of(final DoubleColumn column) {
    return Arrays.stream(columnWriters)
            .filter(columnWriter -> columnWriter.column().equals(column))
            .findFirst()
            .map(columnWriter -> (DoubleColumnWriter) columnWriter)
            .orElseThrow(() -> new NoSuchColumnException(column));
  }

  @Override
  public IdColumnWriter of(final IdColumn column) {
    return Arrays.stream(columnWriters)
            .filter(columnWriter -> columnWriter.column().equals(column))
            .findFirst()
            .map(columnWriter -> (IdColumnWriter) columnWriter)
            .orElseThrow(() -> new NoSuchColumnException(column));
  }

  @Override
  public LongColumnWriter of(final LongColumn column) {
    return Arrays.stream(columnWriters)
            .filter(columnWriter -> columnWriter.column().equals(column))
            .findFirst()
            .map(columnWriter -> (LongColumnWriter) columnWriter)
            .orElseThrow(() -> new NoSuchColumnException(column));
  }

  @Override
  public StringColumnWriter of(final StringColumn column) {
    return Arrays.stream(columnWriters)
            .filter(columnWriter -> columnWriter.column().equals(column))
            .findFirst()
            .map(columnWriter -> (StringColumnWriter) columnWriter)
            .orElseThrow(() -> new NoSuchColumnException(column));
  }

  @Override
  public void next() throws IOException {
    writer.write(columnWriters);
  }

  @Override
  public void flush() {
    // NOOP
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  public static final class BuilderImpl extends Builder<Object, BuilderImpl> {

    private final BaseWriter[] columnWriters;

    private BuilderImpl(final Path path, final BaseWriter[] columnWriters) {
      super(new org.apache.hadoop.fs.Path(path.toAbsolutePath().toUri()));
      this.columnWriters = columnWriters;
      if (isWindows()) {
        var conf = new Configuration();
        // workaround for windows, as hadoop does not work on windows fs
        conf.setClass("fs.file.impl", BareLocalFileSystem.class, FileSystem.class);
        withConf(conf);
      }
    }

    @Override
    protected BuilderImpl self() {
      return this;
    }

    @Override
    protected WriteSupportImpl getWriteSupport(final Configuration conf) {
      return new WriteSupportImpl(columnWriters);
    }

    private static boolean isWindows() {
      return System.getProperty("os.name").startsWith("Windows");
    }
  }
}
