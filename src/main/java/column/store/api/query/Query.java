package column.store.api.query;

import column.store.api.column.Column;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static column.store.util.Conditions.checkArgument;

/**
 * A {@link Query} combines multiple {@link Filter}s, either with {@link QueryType#ALL_OF} (match all {@link Filter}s)
 * or {@link QueryType#AT_LEAST_ONE} (match any {@link Filter}).
 * At least one column has to be selected for a {@link Query} to be valid.
 */
public interface Query {

    /**
     * @return the {@link Filter}s used in the {@link Query}.
     */
    Iterable<Filter> filters();

    /**
     * @return the columns returned by the {@link Query}.
     */
    Iterable<Column> columns();

    /**
     * @return the {@link QueryType} of the {@link Query}.
     */
    QueryType type();

    enum QueryType {
        ALL_OF,
        AT_LEAST_ONE,
    }

    /**
     * @return a new {@link Query.Builder}, which selects the given {@code columns}.
     */
    static Builder select(Column... columns) {
        return new Builder(columns);
    }

    class Builder {

        private final Set<Column> columns = new HashSet<>();
        private final List<Filter> filters = new ArrayList<>();

        private Builder(Column... columns) {
            Collections.addAll(this.columns, columns);
        }

        /**
         * Adds {@code filter} to filters of the {@link Query}.
         */
        public Builder filter(Filter filter) {
            columns.add(filter.column());
            filters.add(filter);
            return this;
        }

        /**
         * @return a new {@link Query}, which will match if <b>all of</b> its filters match.
         */
        public Query allOf() {
            ensureAtLeastOneColumn();
            return new Query() {
                @Override
                public Iterable<Filter> filters() {
                    return filters;
                }

                @Override
                public Iterable<Column> columns() {
                    return columns;
                }

                @Override
                public QueryType type() {
                    return QueryType.ALL_OF;
                }
            };
        }

        /**
         * @return a new {@link Query}, which will match if <b>at least one</b> of its filters match.
         */
        public Query atLeastOne() {
            ensureAtLeastOneColumn();
            return new Query() {
                @Override
                public Iterable<Filter> filters() {
                    return filters;
                }

                @Override
                public Iterable<Column> columns() {
                    return columns;
                }

                @Override
                public QueryType type() {
                    return QueryType.AT_LEAST_ONE;
                }
            };
        }

        private void ensureAtLeastOneColumn() {
            checkArgument(!columns.isEmpty(), "At least one column has to be selected");
        }
    }
}
