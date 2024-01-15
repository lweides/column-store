package column.store;

import column.store.api.read.Reader;
import column.store.csv.read.CSVReader;
import column.store.parquet.read.ParquetReader;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
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

  public static HashMap<String, String> columnTypes(final String yamlFile) {
    try (FileInputStream in = new FileInputStream(yamlFile)) {
      Yaml yaml = new Yaml();
      return yaml.load(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Utils() {
    // hidden util constructor
  }
}
