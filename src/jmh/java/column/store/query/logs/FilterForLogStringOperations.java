package column.store.query.logs;

import column.store.Utils;
import column.store.api.column.Column;
import column.store.api.column.StringColumn;
import column.store.api.query.Query;
import column.store.api.query.StringFilter;
import column.store.api.read.Reader;
import column.store.api.read.StringColumnReader;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Arrays;

import static column.store.api.query.Filter.whereString;

public class FilterForLogStringOperations {

    private static final StringColumn PROCESS_GROUP = Column.forString("dt_entity_process_group_instance-string");
    private static final StringColumn LOG_SOURCE = Column.forString("log_source-string");
    private static final StringColumn PAYLOAD = Column.forString("payload");
    private static final StringColumn HOST_NAME = Column.forString("host_name-string");

    private static final StringColumn[] STRING_COLUMNS = new StringColumn[] {
            PROCESS_GROUP, LOG_SOURCE, PAYLOAD, HOST_NAME
    };


    private static final StringFilter IS_INTERNAL_SPANS = whereString(PROCESS_GROUP).is("PROCESS_GROUP-05EBE2A2E58EAC94");
    private static final StringFilter LOG_SOURCE_CONTAINS = whereString(LOG_SOURCE).contains("joh08775");
    private static final StringFilter PAYLOAD_STARTS_WITH = whereString(PAYLOAD).startsWith("2024-01");
    private static final StringFilter HOST_NAME_ENDS_WITH = whereString(HOST_NAME).endsWith("50006");


    private static final StringFilter[] STRING_FILTERS = new StringFilter[] {
            IS_INTERNAL_SPANS, LOG_SOURCE_CONTAINS, PAYLOAD_STARTS_WITH, HOST_NAME_ENDS_WITH
    };

    @State(Scope.Thread)
    public static class BenchState {
        private Reader reader;
        private Query query;
        private StringColumn[] columns;

        @Param({"parquet", "csv"})
        private String readerType;
        @Param({"true", "false"})
        private boolean isStable;
        @Param({"1", "2", "3", "4"})
        private int numOfCols;

        @Setup(Level.Trial)
        public void setup() {
            reader = Utils.reader(readerType);
            var source = Utils.data("logs", readerType, isStable);

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
