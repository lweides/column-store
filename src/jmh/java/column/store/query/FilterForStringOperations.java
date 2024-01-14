package column.store.query;

import column.store.api.column.*;
import column.store.api.query.Query;
import column.store.api.query.StringFilter;
import column.store.api.read.Reader;
import column.store.api.read.StringColumnReader;
import column.store.util.BenchmarkUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import static column.store.api.query.Filter.whereString;

public class FilterForStringOperations {

    private static final StringColumn SPAN_KIND = Column.forString("span.kind-string");
    private static final StringColumn ENDPOINT_NAME = Column.forString("endpoint.name-string");
    private static final StringColumn SERVLET_CONTEXT_NAME = Column.forString("servlet.context.name-string");
    private static final StringColumn THREAD_NAME = Column.forString("thread.name-string");

    private static final StringColumn[] STRING_COLUMNS = new StringColumn[] {
            SPAN_KIND, ENDPOINT_NAME, SERVLET_CONTEXT_NAME, THREAD_NAME
    };


    private static final StringFilter IS_INTERNAL_SPANS = whereString(SPAN_KIND).is("internal");
    private static final StringFilter CONTAINS_LOCALHOST_ENDPOINT = whereString(ENDPOINT_NAME).contains("localhost");
    private static final StringFilter SERVLET_NAME_STARTS_WITH_EASYTRAVEL = whereString(ENDPOINT_NAME).startsWith("easyTravel");
    private static final StringFilter THREAD_NAME_ENDS_WITH = whereString(ENDPOINT_NAME).endsWith("1172");


    private static final StringFilter[] STRING_FILTERS = new StringFilter[] {
            IS_INTERNAL_SPANS, CONTAINS_LOCALHOST_ENDPOINT, SERVLET_NAME_STARTS_WITH_EASYTRAVEL, THREAD_NAME_ENDS_WITH
    };

    @State(Scope.Thread)
    public static class BenchState {
        public Reader reader;
        public Query query;
        public StringColumn[] columns;
        public Path source;

        @Param({"parquet", "csv"})
        private String readerType;
        @Param({"spans", "logs"})
        private String sourceType;
        @Param({"true", "false"})
        private boolean isStable;
        @Param({"1", "2", "3", "4"})
        private int numOfCols;

        @Setup(Level.Iteration)
        public void setup() {
            reader = BenchmarkUtil.setupReader(readerType);
            source = BenchmarkUtil.setupSource(readerType, sourceType, isStable);

            columns = Arrays.copyOfRange(STRING_COLUMNS, 0, numOfCols);

            var queryBuilder = Query.from(source)
                    .select(columns);
            Arrays.stream(Arrays.copyOfRange(STRING_FILTERS, 0, numOfCols)).forEach(queryBuilder::filter);

            query = queryBuilder.allOf();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void read(final BenchState state, final Blackhole blackhole) throws IOException {
        var reader = state.reader;
        var query = state.query;

        reader.query(query);
        StringColumnReader[] columnReaders = Arrays.stream(state.columns).map(reader::of).toArray(StringColumnReader[]::new);


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
