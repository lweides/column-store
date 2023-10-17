package column.store.api.query;

import column.store.api.column.Column;
import column.store.api.column.StringColumn;

import java.util.Locale;

/**
 * {@link StringFilter} is a case-insensitive {@link Filter}.
 * All filter arguments will be converted to lowercase, based on {@link Locale#ROOT}.
 */
public final class StringFilter extends BaseFilter {

    private final MatchType matchType;
    private final String value;

    private StringFilter(
        Column column,
        MatchType matchType,
        String value
    ) {
        super(column);
        this.matchType = matchType;
        this.value = value;
    }

    public MatchType matchType() {
        return matchType;
    }

    public String value() {
        return value;
    }

    @Override
    public String toString() {
        return "StringFilter[" +
            "column=" + column + ", " +
            "matchType=" + matchType + ", " +
            "value=" + value + ']';
    }

    public enum MatchType {
        IS,
        STARTS_WITH,
        ENDS_WITH,
        CONTAINS,
    }

    public static final class Builder extends Filter.Builder {

        Builder(StringColumn column) {
            super(column);
        }

        /**
         * @return a new {@link StringFilter}, which matches iff {@code record[column] == value}
         */
        public StringFilter is(String value) {
            return new StringFilter(column, MatchType.IS, value.toLowerCase(Locale.ROOT));
        }

        /**
         * @return a new {@link StringFilter}, which matches iff {@code record[column].startsWith(value)}
         */
        public StringFilter startsWith(String value) {
            return new StringFilter(column, MatchType.STARTS_WITH, value.toLowerCase(Locale.ROOT));
        }

        /**
         * @return a new {@link StringFilter}, which matches iff {@code record[column].endsWith(value)}
         */
        public StringFilter endsWith(String value) {
            return new StringFilter(column, MatchType.ENDS_WITH, value.toLowerCase(Locale.ROOT));
        }

        /**
         * @return a new {@link StringFilter}, which matches iff {@code record[column].contains(value)}
         */
        public StringFilter contains(String value) {
            return new StringFilter(column, MatchType.CONTAINS, value.toLowerCase(Locale.ROOT));
        }
    }
}
