package column.store.api.query;

import column.store.api.column.Column;
import column.store.api.column.DoubleColumn;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DoubleFilterTest {

    private final DoubleColumn column = Column.forDouble("some-column");
    @Test
    void lessThanFilterDoesNotSupportLowerBound() {
        assertThatThrownBy(() -> Filter.whereDouble(column).isLessThan(73D).lowerBound())
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("lessThan does not support lowerBound");
    }

    @Test
    void greaterThanFilterDoesNotSupportUpperBound() {
        assertThatThrownBy(() -> Filter.whereDouble(column).isGreaterThan(73D).upperBound())
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("greaterThan does not support upperBound");
    }

    @ParameterizedTest
    @ValueSource(doubles = { 3.14, 17.17 })
    void greaterThanFilter(final double lowerBound) {
        var greaterThan = Filter.whereDouble(column).isGreaterThan(lowerBound);
        assertThat(greaterThan.lowerBound()).isEqualTo(lowerBound);
        assertThat(greaterThan.column()).isEqualTo(column);
        assertThat(greaterThan.matchType()).isEqualTo(DoubleFilter.MatchType.GREATER_THAN);
    }

    @ParameterizedTest
    @ValueSource(doubles = { 3.14, 17.17 })
    void lessThanFilter(final double upperBound) {
        var lessThan = Filter.whereDouble(column).isLessThan(upperBound);
        assertThat(lessThan.upperBound()).isEqualTo(upperBound);
        assertThat(lessThan.column()).isEqualTo(column);
        assertThat(lessThan.matchType()).isEqualTo(DoubleFilter.MatchType.LESS_THAN);

    }

    @Test
    void betweenFilter() {
        var between = Filter.whereDouble(column).isBetween(3.14, 17.17);
        assertThat(between.lowerBound()).isEqualTo(3.14);
        assertThat(between.upperBound()).isEqualTo(17.17);
        assertThat(between.column()).isEqualTo(column);
        assertThat(between.matchType()).isEqualTo(DoubleFilter.MatchType.BETWEEN);
    }

    @Test
    void minHasToBeStrictlyLessThanMax() {
        assertThatThrownBy(() -> Filter.whereDouble(column).isBetween(3, 2))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("minInclusive has to be less than maxExclusive");

        assertThatThrownBy(() -> Filter.whereDouble(column).isBetween(2, 2))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("minInclusive has to be less than maxExclusive");
    }
}