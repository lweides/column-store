package column.store.api.query;

import column.store.api.column.Column;
import column.store.api.column.StringColumn;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

class StringFilterTest {

    private final StringColumn column = Column.forString("some-column");

    @ParameterizedTest
    @ValueSource(strings = { "foo", "bar" })
    void isFilter(String value) {
        var is = Filter.whereString(column).is(value);
        assertThat(is.value()).isEqualTo(value);
        assertThat(is.column()).isEqualTo(column);
        assertThat(is.matchType()).isEqualTo(StringFilter.MatchType.IS);
    }

    @ParameterizedTest
    @ValueSource(strings = { "foo", "bar" })
    void startsWithFilter(String value) {
        var startsWith = Filter.whereString(column).startsWith(value);
        assertThat(startsWith.value()).isEqualTo(value);
        assertThat(startsWith.column()).isEqualTo(column);
        assertThat(startsWith.matchType()).isEqualTo(StringFilter.MatchType.STARTS_WITH);
    }

    @ParameterizedTest
    @ValueSource(strings = { "foo", "bar" })
    void endsWithFilter(String value) {
        var endsWith = Filter.whereString(column).endsWith(value);
        assertThat(endsWith.value()).isEqualTo(value);
        assertThat(endsWith.column()).isEqualTo(column);
        assertThat(endsWith.matchType()).isEqualTo(StringFilter.MatchType.ENDS_WITH);
    }

    @ParameterizedTest
    @ValueSource(strings = { "foo", "bar" })
    void containsFilter(String value) {
        var contains = Filter.whereString(column).contains(value);
        assertThat(contains.value()).isEqualTo(value);
        assertThat(contains.column()).isEqualTo(column);
        assertThat(contains.matchType()).isEqualTo(StringFilter.MatchType.CONTAINS);
    }

    @ParameterizedTest
    @ValueSource(strings = { "FoO", "HelLO WORLd" })
    void stringsAreConvertedToLowercase(String mixedCase) {
        assertThat(Filter.whereString(column).is(mixedCase).value())
            .isEqualTo(mixedCase.toLowerCase(Locale.ROOT));

        assertThat(Filter.whereString(column).startsWith(mixedCase).value())
            .isEqualTo(mixedCase.toLowerCase(Locale.ROOT));

        assertThat(Filter.whereString(column).endsWith(mixedCase).value())
            .isEqualTo(mixedCase.toLowerCase(Locale.ROOT));

        assertThat(Filter.whereString(column).contains(mixedCase).value())
            .isEqualTo(mixedCase.toLowerCase(Locale.ROOT));
    }
}