package column.store.api.query;

import column.store.api.Utils;
import column.store.api.column.Column;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterTest {

    @Test
    void allFiltersAreTested() {
        var filters = Utils.subclasses(Column.class);
        assertThat(filters).hasSize(7); // Filter, BaseFilter, BooleanFilter, DoubleFilter, IdFilter, LongFilter, StringFilter
    }
}
