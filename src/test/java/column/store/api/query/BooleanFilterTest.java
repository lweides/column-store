package column.store.api.query;

import column.store.api.column.BooleanColumn;
import column.store.api.column.Column;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class BooleanFilterTest {

    private final BooleanColumn column = Column.forBoolean("some-column");

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void isFilter(final boolean value) {
        var is = Filter.whereBoolean(column).is(value);
        assertThat(is.value()).isEqualTo(value);
        assertThat(is.column()).isEqualTo(column);
    }
}