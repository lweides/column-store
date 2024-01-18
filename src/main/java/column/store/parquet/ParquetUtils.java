package column.store.parquet;

import column.store.api.column.Column;

import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem;

public final class ParquetUtils {

  public static String schemaFrom(final Stream<Column> columns) {
    var sb = new StringBuilder("message record {");
    columns.forEach(column -> {
      sb
              .append("optional ")
              .append(parquetTypeFrom(column))
              .append(" ")
              .append(column.name())
              .append(";");
    });
    sb.append("}");
    return sb.toString();
  }

  private static String parquetTypeFrom(final Column column) {
    return switch (column.type()) {
      case BOOLEAN -> "boolean";
      case DOUBLE -> "double";
      case ID, STRING -> "binary";
      case LONG -> "int64";
    };
  }

  public static void patchConfigForWindows(final Configuration configuration) {
    if (isWindows()) {
      // workaround for windows, as hadoop does not work on windows fs
      configuration.setClass("fs.file.impl", BareLocalFileSystem.class, FileSystem.class);
    } else {
      // whatever this is: https://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
      configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }
  }

  private static boolean isWindows() {
    return System.getProperty("os.name").startsWith("Windows");
  }

  private ParquetUtils() {
    // hidden util constructor
  }
}
