package column.store;

import column.store.api.read.Reader;
import column.store.csv.read.CSVReader;
import column.store.parquet.read.ParquetReader;

import java.nio.file.Path;
import java.util.function.UnaryOperator;

public final class Utils {

  private static final UnaryOperator<org.apache.parquet.hadoop.ParquetReader.Builder<Object>> CONFIG = builder -> builder
          .useDictionaryFilter()
          .useBloomFilter()
          .useColumnIndexFilter()
          .useDictionaryFilter();

  public static Reader reader(final String type) {
    return switch (type) {
      case "csv" -> new CSVReader();
      case "parquet" -> new ParquetReader(CONFIG);
      default -> throw new IllegalArgumentException("Unsupported type: " + type);
    };
  }

  public static Path data(final String source, final String type, final boolean isStable) {
    var base = Path.of("datasets", source, isStable ? "stable" : "unstable");
    return switch (type) {
      case "csv" -> base.resolve(Path.of("csv", "data.csv"));
      case "parquet" -> base.resolve("parquet");
      default -> throw new IllegalArgumentException("Unsupported type: " + type);
    };
  }

  private Utils() {
    // hidden util constructor
  }
}
