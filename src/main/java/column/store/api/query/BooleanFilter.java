package column.store.api.query;

import column.store.api.column.BooleanColumn;
import column.store.api.column.Column;

public final class BooleanFilter extends BaseFilter {

    private final boolean value;

    private BooleanFilter(Column column, boolean value) {
        super(column);
        this.value = value;
    }

    public boolean value() {
        return value;
    }

    @Override
    public String toString() {
        return "BooleanFilter[column=" + column + ", value=" + value + ']';
    }

    public static final class Builder extends Filter.Builder {

        Builder(BooleanColumn column) {
            super(column);
        }

        /**
         * @return a new {@link BooleanFilter}, which matches a record iff {@code record[column] == value}.
         */
        BooleanFilter is(boolean value) {
            return new BooleanFilter(column, value);
        }
    }
}
