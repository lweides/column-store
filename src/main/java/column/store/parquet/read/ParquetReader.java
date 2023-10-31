package column.store.parquet.read;

import column.store.api.column.*;
import column.store.api.query.BooleanFilter;
import column.store.api.query.DoubleFilter;
import column.store.api.query.Filter;
import column.store.api.query.IdFilter;
import column.store.api.query.LongFilter;
import column.store.api.query.Query;
import column.store.api.query.StringFilter;
import column.store.api.read.*;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.userDefined;
import static org.apache.parquet.hadoop.ParquetReader.builder;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.io.api.Binary;

public class ParquetReader implements Reader {

    private final org.apache.hadoop.fs.Path path;
    private final Map<String, BaseReader> readers = new HashMap<>();
    private final ReadSupportImpl readSupport = new ReadSupportImpl(readers);
    private org.apache.parquet.hadoop.ParquetReader<Object> parquetReader;

    private boolean consumed = true;
    private boolean hasNext;

    public ParquetReader(final Path path) {
        this.path = new org.apache.hadoop.fs.Path(path.toUri());
    }

    @Override
    public void query(final Query query) throws IOException {
        // reset from a previous query
        readers.clear();
        close();

        query.columns().forEach(column -> readers.put(column.name(), switch (column.type()) {
            case BOOLEAN -> new BooleanReader();
            case DOUBLE -> new DoubleReader();
            case ID -> new IdReader();
            case LONG -> new LongReader();
            case STRING -> new StringReader();
        }));

        var builder = builder(readSupport, path);

        if (query.filters().iterator().hasNext()) {
            var filters = new ArrayList<FilterPredicate>();
            query.filters().forEach(filter -> filters.add(convert(filter)));
            switch (query.type()) {
                case ALL_OF -> builder.withFilter(FilterCompat.get(and(filters)));
                case AT_LEAST_ONE -> builder.withFilter(FilterCompat.get(or(filters)));
                default -> throw new IllegalArgumentException("Unknown query type: " + query.type());
            }
        }

        parquetReader = builder.build();
    }

    @Override
    public BooleanColumnReader of(final BooleanColumn column) {
        return Optional.ofNullable(readers.get(column.name()))
                .map(BooleanColumnReader.class::cast)
                .orElseThrow(() -> new NoSuchColumnException(column));
    }

    @Override
    public DoubleColumnReader of(final DoubleColumn column) {
        return Optional.ofNullable(readers.get(column.name()))
                .map(DoubleColumnReader.class::cast)
                .orElseThrow(() -> new NoSuchColumnException(column));
    }

    @Override
    public IdColumnReader of(final IdColumn column) {
        return Optional.ofNullable(readers.get(column.name()))
                .map(IdColumnReader.class::cast)
                .orElseThrow(() -> new NoSuchColumnException(column));
    }

    @Override
    public LongColumnReader of(final LongColumn column) {
        return Optional.ofNullable(readers.get(column.name()))
                .map(LongColumnReader.class::cast)
                .orElseThrow(() -> new NoSuchColumnException(column));
    }

    @Override
    public StringColumnReader of(final StringColumn column) {
        return Optional.ofNullable(readers.get(column.name()))
                .map(StringColumnReader.class::cast)
                .orElseThrow(() -> new NoSuchColumnException(column));
    }

    @Override
    public boolean hasNext() throws IOException {
        if (!consumed) {
            return hasNext;
        }
        consumed = false;
        readers.values().forEach(BaseReader::reset);
        hasNext = parquetReader.read() != null;
        return hasNext;
    }

    @Override
    public void next() throws IOException {
        if (!consumed && !hasNext) {
            throw new NoSuchElementException("No next value");
        }


        if (consumed && !hasNext()) {
            throw new NoSuchElementException("No next value");
        }

        assert !consumed;
        consumed = true;
    }

    @Override
    public void close() throws IOException {
        if (parquetReader != null) {
            parquetReader.close();
        }
    }

    private static FilterPredicate convert(final Filter filter) {
        return switch (filter) {
            case BooleanFilter booleanFilter -> convert(booleanFilter);
            case DoubleFilter doubleFilter -> convert(doubleFilter);
            case IdFilter idFilter -> convert(idFilter);
            case LongFilter longFilter -> convert(longFilter);
            case StringFilter stringFilter -> convert(stringFilter);
        };
    }

    private static FilterPredicate convert(final BooleanFilter filter) {
        return eq(booleanColumn(filter.column().name()), filter.value());
    }

    private static FilterPredicate convert(final DoubleFilter filter) {
        var name = filter.column().name();
        return switch (filter.matchType()) {
            case LESS_THAN -> lt(doubleColumn(name), filter.upperBound());
            case GREATER_THAN -> gt(doubleColumn(name), filter.lowerBound());
            case BETWEEN ->
                    FilterApi.and(gtEq(doubleColumn(name), filter.lowerBound()), lt(doubleColumn(name), filter.upperBound()));
        };
    }

    private static FilterPredicate convert(final IdFilter filter) {
        return eq(binaryColumn(filter.column().name()), Binary.fromConstantByteArray(filter.id()));
    }

    private static FilterPredicate convert(final LongFilter filter) {
        var name = filter.column().name();
        return switch (filter.matchType()) {
            case LESS_THAN -> lt(longColumn(name), filter.upperBound());
            case GREATER_THAN -> gt(longColumn(name), filter.lowerBound());
            case BETWEEN ->
                    FilterApi.and(gtEq(longColumn(name), filter.lowerBound()), lt(longColumn(name), filter.upperBound()));
        };
    }

    private static FilterPredicate convert(final StringFilter filter) {
        return userDefined(FilterApi.binaryColumn(filter.column().name()), new StringPredicate(filter));
    }

    private static FilterPredicate and(final List<FilterPredicate> filters) {
        var current = filters.get(0);
        for (int i = 1; i < filters.size(); i++) {
            current = FilterApi.and(current, filters.get(i));
        }
        return current;
    }

    private static FilterPredicate or(final List<FilterPredicate> filters) {
        var current = filters.get(0);
        for (int i = 1; i < filters.size(); i++) {
            current = FilterApi.or(current, filters.get(i));
        }
        return current;
    }

    private static final class StringPredicate extends UserDefinedPredicate<Binary> implements Serializable {

        private final StringFilter filter;

        private StringPredicate(final StringFilter filter) {
            this.filter = filter;
        }

        @Override
        public boolean keep(final Binary value) {
            return switch (filter.matchType()) {
                case IS -> value.toStringUsingUTF8().toLowerCase(Locale.ROOT).equals(filter.value());
                case STARTS_WITH -> value.toStringUsingUTF8().toLowerCase(Locale.ROOT).startsWith(filter.value());
                case ENDS_WITH -> value.toStringUsingUTF8().toLowerCase(Locale.ROOT).endsWith(filter.value());
                case CONTAINS -> value.toStringUsingUTF8().toLowerCase(Locale.ROOT).contains(filter.value());
            };
        }

        @Override
        public boolean canDrop(final Statistics<Binary> statistics) {
            return false;
        }

        @Override
        public boolean inverseCanDrop(final Statistics<Binary> statistics) {
            return false;
        }
    }
}
