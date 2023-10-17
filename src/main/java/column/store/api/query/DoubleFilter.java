package column.store.api.query;

import column.store.api.column.Column;
import column.store.api.column.DoubleColumn;

import static column.store.util.Conditions.checkArgument;
import static column.store.util.Conditions.checkSupported;

public final class DoubleFilter extends BaseFilter {

    private final MatchType matchType;
    private final double lowerBound;
    private final double upperBound;

    private DoubleFilter(Column column, MatchType matchType, double lowerBound, double upperBound) {
        super(column);
        this.matchType = matchType;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public MatchType matchType() {
        return matchType;
    }

    public double lowerBound() {
        checkSupported(matchType != MatchType.LESS_THAN, "lessThan does not support lowerBound");
        return lowerBound;
    }

    public double upperBound() {
        checkSupported(matchType != MatchType.GREATER_THAN, "greaterThan does not support upperBound");
        return upperBound;
    }

    @Override
    public String toString() {
        return "DoubleFilter[" +
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

        Builder(DoubleColumn column) {
            super(column);
        }

        /**
         * @return a new {@link DoubleFilter}, which matches a record iff {@code record[column] < maxExclusive}.
         */
        public DoubleFilter isLessThan(double maxExclusive) {
            return new DoubleFilter(column, MatchType.LESS_THAN, Double.MIN_VALUE, maxExclusive);
        }

        /**
         * @return a new {@link DoubleFilter}, which matches a record iff {@code record[column] > minExclusive}.
         */
        public DoubleFilter isGreaterThan(double minExclusive) {
            return new DoubleFilter(column, MatchType.GREATER_THAN, minExclusive, Double.MAX_VALUE);
        }

        /**
         * @return a new {@link DoubleFilter}, which matches a record iff {@code minInclusive <= record[column] < maxExclusive}.
         */
        public DoubleFilter isBetween(double minInclusive, double maxExclusive) {
            checkArgument(minInclusive < maxExclusive, "minInclusive has to be less than maxExclusive");
            return new DoubleFilter(column, MatchType.BETWEEN, minInclusive, maxExclusive);
        }
    }
}
