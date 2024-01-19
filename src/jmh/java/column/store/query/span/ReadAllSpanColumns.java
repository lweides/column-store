package column.store.query.span;

import column.store.Utils;
import column.store.api.column.*;
import column.store.api.query.Query;
import column.store.api.read.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ReadAllSpanColumns {

    @State(Scope.Thread)
    public static class BenchState {
        private Reader reader;
        private Query query;
        private Path source;
        private HashMap<String, String> columnTypes;

        private List<StringColumnReader> stringReaders;
        private List<BooleanColumnReader> booleanReaders;
        private List<IdColumnReader> idReaders;
        private List<LongColumnReader> longReaders;
        private List<DoubleColumnReader> doubleReaders;

        @Param({"parquet", "csv"})
        private String readerType;
        @Param({"true", "false"})
        private boolean isStable;


        @Setup(Level.Trial)
        public void setup() {
            reader = Utils.reader(readerType);
            source = Utils.data("spans", readerType, isStable);
            columnTypes = Utils.columnTypes("spanColumnTypes.yaml");

            query = Query.from(source)
                    .selectAll()
                    .allOf();

            var columnMap = reader.columnNames()
                    .stream()
                    .map(columnName -> columnOf(columnTypes.get(columnName), columnName))
                    .collect(Collectors.groupingBy(Column::type));

            stringReaders = Optional.ofNullable(columnMap.get(Column.Type.STRING))
                    .orElse(new ArrayList<>())
                    .stream()
                    .map(col -> reader.of((StringColumn) col))
                    .toList();

            booleanReaders = Optional.ofNullable(columnMap.get(Column.Type.BOOLEAN))
                    .orElse(new ArrayList<>())
                    .stream()
                    .map(col -> reader.of((BooleanColumn) col))
                    .toList();

            idReaders = Optional.ofNullable(columnMap.get(Column.Type.ID))
                    .orElse(new ArrayList<>())
                    .stream()
                    .map(col -> reader.of((IdColumn) col))
                    .toList();

            longReaders = Optional.ofNullable(columnMap.get(Column.Type.LONG))
                    .orElse(new ArrayList<>())
                    .stream()
                    .map(col -> reader.of((LongColumn) col))
                    .toList();

            doubleReaders = Optional.ofNullable(columnMap.get(Column.Type.DOUBLE))
                    .orElse(new ArrayList<>())
                    .stream()
                    .map(col -> reader.of((DoubleColumn) col))
                    .toList();
        }

        private Column columnOf(final String columnType, final String name) {
            return switch (columnType) {
                case "BOOLEAN" -> Column.forBoolean(name);
                case "DOUBLE" -> Column.forDouble(name);
                case "ID" -> Column.forId(name);
                case "LONG" -> Column.forLong(name);
                case "STRING" -> Column.forString(name);
                default -> throw new IllegalArgumentException("Unsupported type: " + columnType);
            };
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
    public void read(final BenchState state, final Blackhole blackhole) throws IOException {
        var reader = state.reader;
        var query = state.query;

        reader.query(query);

        var stringReaders = state.stringReaders;
        var booleanReaders = state.booleanReaders;
        var idReaders = state.idReaders;
        var longReaders = state.longReaders;
        var doubleReaders = state.doubleReaders;


        while (reader.hasNext()) {
            reader.next();
            stringReaders.forEach(r -> {
                if (r.isPresent()) {
                    blackhole.consume(r.get());
                }
            });

            booleanReaders.forEach(r -> {
                if (r.isPresent()) {
                    blackhole.consume(r.get());
                }
            });

            idReaders.forEach(r -> {
                if (r.isPresent()) {
                    blackhole.consume(r.get());
                }
            });

            longReaders.forEach(r -> {
                if (r.isPresent()) {
                    blackhole.consume(r.get());
                }
            });

            doubleReaders.forEach(r -> {
                if (r.isPresent()) {
                    blackhole.consume(r.get());
                }
            });
        }
    }
}
