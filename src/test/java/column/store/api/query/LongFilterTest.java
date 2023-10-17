package column.store.api.query;

import column.store.api.column.Column;
import column.store.api.column.LongColumn;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LongFilterTest {

    private final LongColumn column = Column.forLong("some-columN");

    @Test
    void lessThanFilterDoesNotSupportLowerBound() {
        assertThatThrownBy(() -> Filter.whereLong(column).isLessThan(73L).lowerBound())
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("lessThan does not support lowerBound");
    }

    @Test
    void greaterThanFilterDoesNotSupportUpperBound() {
        assertThatThrownBy(() -> Filter.whereLong(column).isGreaterThan(73L).upperBound())
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("greaterThan does not support upperBound");
    }

    @ParameterizedTest
    @ValueSource(longs = { 42L, 73L })
    void greaterThanFilter(long lowerBound) {
        var greaterThan = Filter.whereLong(column).isGreaterThan(lowerBound);
        assertThat(greaterThan.lowerBound()).isEqualTo(lowerBound);
        assertThat(greaterThan.column()).isEqualTo(column);
        assertThat(greaterThan.matchType()).isEqualTo(LongFilter.MatchType.GREATER_THAN);
    }

    @ParameterizedTest
    @ValueSource(longs = { 42L, 73L })
    void lessThanFilter(long upperBound) {
        var lessThan = Filter.whereLong(column).isLessThan(upperBound);
        assertThat(lessThan.upperBound()).isEqualTo(upperBound);
        assertThat(lessThan.column()).isEqualTo(column);
        assertThat(lessThan.matchType()).isEqualTo(LongFilter.MatchType.LESS_THAN);
    }

    @Test
    void betweenFilter() {
        var between = Filter.whereLong(column).isBetween(13L, 17L);
        assertThat(between.lowerBound()).isEqualTo(13L);
        assertThat(between.upperBound()).isEqualTo(17L);
        assertThat(between.column()).isEqualTo(column);
        assertThat(between.matchType()).isEqualTo(LongFilter.MatchType.BETWEEN);
    }

    @Test
    void minHasToBeStrictlyLessThanMax() {
        assertThatThrownBy(() -> Filter.whereLong(column).isBetween(3, 2))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("minInclusive has to be less than maxExclusive");

        assertThatThrownBy(() -> Filter.whereLong(column).isBetween(2, 2))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("minInclusive has to be less than maxExclusive");
    }
}