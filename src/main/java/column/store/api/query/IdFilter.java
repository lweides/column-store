package column.store.api.query;

import column.store.api.column.Column;
import column.store.api.column.IdColumn;

import java.util.Arrays;

public final class IdFilter extends BaseFilter {

    private final byte[] id;

    private IdFilter(Column column, byte[] id) {
        super(column);
        this.id = id;
    }

    public byte[] id() {
        return id;
    }

    @Override
    public String toString() {
        return "IdFilter[column=" + column + ", id=" + Arrays.toString(id) + ']';
    }

    public static final class Builder extends Filter.Builder {

        Builder(IdColumn column) {
            super(column);
        }

        /**
         * @return a new {@link BooleanFilter}, which matches a record iff {@code record[column] == id}.
         */
        public IdFilter is(byte[] id) {
            return new IdFilter(column, id);
        }
    }
}
