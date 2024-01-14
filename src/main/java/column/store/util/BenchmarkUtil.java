package column.store.util;

import column.store.api.read.Reader;
import column.store.csv.read.CSVReader;
import column.store.parquet.read.ParquetReader;

import java.nio.file.Path;
import java.util.Locale;
import java.util.function.UnaryOperator;

public class BenchmarkUtil {

    public static Reader setupReader(String readerType) {
        return switch (readerType.toLowerCase(Locale.ROOT)) {
            case "parquet" -> {
                UnaryOperator<org.apache.parquet.hadoop.ParquetReader.Builder<Object>> config = UnaryOperator.identity();
                yield new ParquetReader(config);
            }
            case "csv" -> new CSVReader();

            default -> throw new IllegalStateException("Unexpected state for parameter readerType: " + readerType);
        };
    }

    public static Path setupSource(String readerType, String sourceType, boolean isStable) {

        String reader = switch (readerType.toLowerCase(Locale.ROOT)) {
            case "parquet" -> "PARQUET";
            case "csv" -> "CSV";

            default -> throw new IllegalStateException("Unexpected state for parameter readerType: " + readerType);
        };

        String source = switch (sourceType) {
            case "spans" -> "SPANS";
            case "logs" -> "LOGS";

            default -> throw new IllegalStateException("Unexpected state for parameter sourceType: " + sourceType);
        };

        String directory = String.format("bench_%s_%s_%s", reader, source, isStable ? "STABLE" : "UNSTABLE");
        Path path = Path.of("..", "column-store-tools", directory);
        return reader.equals("PARQUET") ? path : path.resolve("data.csv");
    }
}
