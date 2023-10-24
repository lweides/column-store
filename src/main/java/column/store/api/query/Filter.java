package column.store.api.query;

import column.store.api.column.BooleanColumn;
import column.store.api.column.Column;
import column.store.api.column.DoubleColumn;
import column.store.api.column.IdColumn;
import column.store.api.column.LongColumn;
import column.store.api.column.StringColumn;

/**
 * A {@link Filter} is a typed predicate for a single {@link Column}.
 * Supported filter types are:
 * <ul>
 *     <li>{@link BooleanFilter}</li>
 *     <li>{@link DoubleFilter}</li>
 *     <li>{@link IdFilter}</li>
 *     <li>{@link LongFilter}</li>
 *     <li>{@link StringFilter}</li>
 * </ul>
 *
 * A {@link Filter} can be constructed using the utility methods in this class.
 */
public sealed interface Filter permits BaseFilter {

    /**
     * @return the {@link Column} on which this {@link Filter} acts.
     */
    Column column();

    /**
     * @return a new {@link BooleanFilter.Builder}, which acts on {@code column}.
     */
    static BooleanFilter.Builder whereBoolean(final BooleanColumn column) {
        return new BooleanFilter.Builder(column);
    }

    /**
     * @return a new {@link DoubleFilter.Builder}, which acts on {@code column}.
     */
    static DoubleFilter.Builder whereDouble(final DoubleColumn column) {
        return new DoubleFilter.Builder(column);
    }

    /**
     * @return a new {@link IdFilter.Builder}, which acts on {@code column}.
     */
    static IdFilter.Builder whereId(final IdColumn column) {
        return new IdFilter.Builder(column);
    }

    /**
     * @return a new {@link LongFilter.Builder}, which acts on {@code column}.
     */
    static LongFilter.Builder whereLong(final LongColumn column) {
        return new LongFilter.Builder(column);
    }

    /**
     * @return a new {@link StringFilter.Builder}, which acts on {@code column}.
     */
    static StringFilter.Builder whereString(final StringColumn column) {
        return new StringFilter.Builder(column);
    }

    abstract sealed class Builder
            permits BooleanFilter.Builder, DoubleFilter.Builder, IdFilter.Builder, LongFilter.Builder, StringFilter.Builder {

        protected final Column column;

        Builder(final Column column) {
            this.column = column;
        }
    }
}
