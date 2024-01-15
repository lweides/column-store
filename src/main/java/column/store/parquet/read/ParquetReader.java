package column.store.parquet.read;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.UnaryOperator;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.Binary;

import column.store.api.column.BooleanColumn;
import column.store.api.column.DoubleColumn;
import column.store.api.column.IdColumn;
import column.store.api.column.LongColumn;
import column.store.api.column.StringColumn;
import column.store.api.query.BooleanFilter;
import column.store.api.query.DoubleFilter;
import column.store.api.query.Filter;
import column.store.api.query.IdFilter;
import column.store.api.query.LongFilter;
import column.store.api.query.Query;
import column.store.api.query.StringFilter;
import column.store.api.read.BooleanColumnReader;
import column.store.api.read.DoubleColumnReader;
import column.store.api.read.IdColumnReader;
import column.store.api.read.LongColumnReader;
import column.store.api.read.Reader;
import column.store.api.read.StringColumnReader;
import column.store.parquet.ParquetUtils;

@SuppressWarnings("checkstyle:OperatorWrap")
public class ParquetReader implements Reader {

    private final Map<String, BaseReader> readers = new HashMap<>();
    private final ReadSupportImpl readSupport = new ReadSupportImpl(readers);
    private final UnaryOperator<org.apache.parquet.hadoop.ParquetReader.Builder<Object>> config;
    private org.apache.parquet.hadoop.ParquetReader<Object> parquetReader;

    private boolean consumed = true;
    private boolean hasNext;

  public ParquetReader(final UnaryOperator<org.apache.parquet.hadoop.ParquetReader.Builder<Object>> config) {
    this.config = config;
  }

  @Override
    public void query(final Query query) throws IOException {
        // reset from a previous query
        readers.clear();
        close();

        var path = new Path(query.filePath().toUri());
        var builder = config.apply(builder(readSupport, path));

        var conf = new Configuration();
        ParquetUtils.patchConfigForWindows(conf);

        if (!query.selectAll()) {
          // set read schema to only read the requested columns
          var schema = ParquetUtils.schemaFrom(StreamSupport.stream(query.columns().spliterator(), false));
          conf.set(ReadSupport.PARQUET_READ_SCHEMA, schema);
        }

        builder.withConf(conf);

        switch (query.type()) {
            case ALL_OF -> builder.withFilter(and(query.filters()));
            case AT_LEAST_ONE -> builder.withFilter(or(query.filters()));
            default -> throw new IllegalArgumentException("Unsupported filter type: " + query.type());
        }

        parquetReader = builder.build();
    }

    @Override
    public BooleanColumnReader of(final BooleanColumn column) {
        return (BooleanColumnReader) readers.computeIfAbsent(column.name(), n -> new BooleanReader());
    }

    @Override
    public DoubleColumnReader of(final DoubleColumn column) {
      return (DoubleColumnReader) readers.computeIfAbsent(column.name(), n -> new DoubleReader());
    }

    @Override
    public IdColumnReader of(final IdColumn column) {
      return (IdColumnReader) readers.computeIfAbsent(column.name(), n -> new IdReader());
    }

    @Override
    public LongColumnReader of(final LongColumn column) {
      return (LongColumnReader) readers.computeIfAbsent(column.name(), n -> new LongReader());
    }

    @Override
    public StringColumnReader of(final StringColumn column) {
      return (StringColumnReader) readers.computeIfAbsent(column.name(), n -> new StringReader());
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

    private static FilterCompat.Filter and(final Collection<Filter> filters) {
        return filters.stream()
                .map(ParquetReader::convert)
                .reduce(FilterApi::and)
                .map(FilterCompat::get)
                .orElse(FilterCompat.NOOP);
    }

    private static FilterCompat.Filter or(final Collection<Filter> filters) {
        return filters.stream()
                .map(ParquetReader::convert)
                .reduce(FilterApi::or)
                .map(FilterCompat::get)
                .orElse(FilterCompat.NOOP);
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
