package column.store.api.query;

import column.store.api.column.Column;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static column.store.api.query.Filter.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryTest {

    private static final Path DUMMY_FILEPATH = Path.of("");

    @Test
    void allFiltersAreAdded() {
        var doubleColumn = Column.forDouble("some-column");
        var longColumn = Column.forLong("another-column");
        var stringColumn = Column.forString("yet-another-column");

        var lessThan = whereDouble(doubleColumn).isLessThan(3.14);
        var greaterThan = whereLong(longColumn).isGreaterThan(73);
        var contains = whereString(stringColumn).contains("foo");

        var query = Query.from(DUMMY_FILEPATH)
                .select(doubleColumn, longColumn, stringColumn)
                .filter(lessThan)
                .filter(greaterThan)
                .filter(contains)
                .allOf();

        assertThat(query.filters()).contains(lessThan, greaterThan, contains);
    }

    @Test
    void columnsByFiltersAreAppended() {
        var someColumn = Column.forId("some-column");
        var columnNotInSelect = Column.forBoolean("column-not-in-select");
        var query = Query.from(DUMMY_FILEPATH)
                .select(someColumn)
                .filter(whereBoolean(columnNotInSelect).is(true))
                .allOf();

        assertThat(query.columns()).contains(someColumn, columnNotInSelect);
    }

    @Test
    void atLeastOneColumnHasToBeSelected() {
        assertThatThrownBy(() -> Query.from(DUMMY_FILEPATH).select().allOf())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("At least one column has to be selected");
    }
}