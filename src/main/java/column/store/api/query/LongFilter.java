package column.store.api.query;

import column.store.api.column.Column;
import column.store.api.column.LongColumn;

import static column.store.util.Conditions.checkArgument;
import static column.store.util.Conditions.checkSupported;

public final class LongFilter extends BaseFilter {

    private final MatchType matchType;
    private final long lowerBound;
    private final long upperBound;

    private LongFilter(final Column column, final MatchType matchType, final long lowerBound, final long upperBound) {
        super(column);
        this.matchType = matchType;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public MatchType matchType() {
        return matchType;
    }

    public long lowerBound() {
        checkSupported(matchType != MatchType.LESS_THAN, "lessThan does not support lowerBound");
        return lowerBound;
    }

    public long upperBound() {
        checkSupported(matchType != MatchType.GREATER_THAN, "greaterThan does not support upperBound");
        return upperBound;
    }

    @Override
    public String toString() {
        return "LongFilter[" +
            "column=" + column + ", " +
            "matchType=" + matchType + ", " +
            "lowerBound=" + lowerBound + ", " +
            "upperBound=" + upperBound + ']';
    }

    public enum MatchType {
        LESS_THAN,
        GREATER_THAN,
        BETWEEN,
    }

    public static final class Builder extends Filter.Builder {

        Builder(final LongColumn column) {
            super(column);
        }

        /**
         * @return a new {@link LongFilter}, which matches a record iff {@code record[column] < maxExclusive}.
         */
        public LongFilter isLessThan(final long maxExclusive) {
            return new LongFilter(column, MatchType.LESS_THAN, Long.MIN_VALUE, maxExclusive);
        }

        /**
         * @return a new {@link LongFilter}, which matches a record iff {@code record[column] > minExclusive}.
         */
        public LongFilter isGreaterThan(final long minExclusive) {
            return new LongFilter(column, MatchType.GREATER_THAN, minExclusive, Long.MAX_VALUE);
        }

        /**
         * @return a new {@link LongFilter}, which matches a record iff {@code minInclusive <= record[column] < maxExclusive}.
         */
        public LongFilter isBetween(final long minInclusive, final long maxExclusive) {
             checkArgument(minInclusive < maxExclusive, "minInclusive has to be less than maxExclusive");
            return new LongFilter(column, MatchType.BETWEEN, minInclusive, maxExclusive);
        }
    }
}
