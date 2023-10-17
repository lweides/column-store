package column.store.api.column;

import column.store.api.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Locale;

import static column.store.api.column.Column.Type.BOOLEAN;
import static column.store.api.column.Column.Type.DOUBLE;
import static column.store.api.column.Column.Type.ID;
import static column.store.api.column.Column.Type.LONG;
import static column.store.api.column.Column.Type.STRING;
import static org.assertj.core.api.Assertions.assertThat;

class ColumnTest {

    @ParameterizedTest
    @ValueSource(strings = { "someColumn", "YET_ANOTHER_COLUMN", "aVeRyWeIrDcOlUmN"})
    void columnNamesAreLowercase(String name) {
        var booleanColumn = Column.forBoolean(name);
        var doubleColumn = Column.forDouble(name);
        var idColumn = Column.forId(name);
        var longColumn = Column.forLong(name);
        var stringColumn = Column.forString(name);

        assertThat(booleanColumn.name()).isEqualTo(name.toLowerCase(Locale.ROOT));
        assertThat(doubleColumn.name()).isEqualTo(name.toLowerCase(Locale.ROOT));
        assertThat(idColumn.name()).isEqualTo(name.toLowerCase(Locale.ROOT));
        assertThat(longColumn.name()).isEqualTo(name.toLowerCase(Locale.ROOT));
        assertThat(stringColumn.name()).isEqualTo(name.toLowerCase(Locale.ROOT));
    }

    @Test
    void columnsHaveProperType() {
        assertThat(Column.forBoolean("boolean").type()).isEqualTo(BOOLEAN);
        assertThat(Column.forDouble("double").type()).isEqualTo(DOUBLE);
        assertThat(Column.forId("id").type()).isEqualTo(ID);
        assertThat(Column.forLong("long").type()).isEqualTo(LONG);
        assertThat(Column.forString("string").type()).isEqualTo(STRING);
    }

    @Test
    void allColumnsAreTested() {
        var columns = Utils.subclasses(Column.class);
        assertThat(columns).hasSize(7); // Column, BaseColumn, BooleanColumn, DoubleColumn, IdColumn, LongColumn, StringColumn
    }
}