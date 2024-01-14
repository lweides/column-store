package column.store.query;

import column.store.api.column.Column;
import column.store.api.query.Query;
import column.store.api.read.Reader;
import column.store.api.read.StringColumnReader;
import column.store.util.BenchmarkUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Path;


public class ReadAllColumns {

    @State(Scope.Thread)
    public static class BenchState {
        public Reader reader;
        public Query query;
        public Path source;

        @Param({"parquet", "csv"})
        private String readerType;
        @Param({"spans", "logs"})
        private String sourceType;
        @Param({"true", "false"})
        private boolean isStable;


        @Setup(Level.Iteration)
        public void setup() {
            reader = BenchmarkUtil.setupReader(readerType);
            source = BenchmarkUtil.setupSource(readerType, sourceType, isStable);


            query = Query.from(source)
                    .selectAll().
                    allOf();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void read(final FilterForStringOperations.BenchState state, final Blackhole blackhole) throws IOException {
        var reader = state.reader;
        var query = state.query;

        reader.query(query);
        StringColumnReader[] columnReaders = reader.columnNames()
                .stream()
                .map(col -> reader.of(Column.forString(col)))
                .toArray(StringColumnReader[]::new);


        while (reader.hasNext()) {
            reader.next();

            for (StringColumnReader columnReader : columnReaders) {
                if (columnReader.isPresent()) {
                    blackhole.consume(columnReader.get());
                }
            }
        }
    }
}
