package column.store.api.query;

import column.store.api.column.Column;

abstract sealed class BaseFilter implements Filter permits BooleanFilter, DoubleFilter, IdFilter, LongFilter, StringFilter {

    protected final Column column;

    BaseFilter(Column column) {
        this.column = column;
    }

    @Override
    public Column column() {
        return column;
    }
}
