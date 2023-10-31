package column.store.api.read;

import column.store.api.column.Column;

/**
 * A {@link ColumnReader} is a typed reader for a single {@link Column}.
 * Supported reader types are:
 * <ul>
 *     <li>{@link BooleanColumnReader}</li>
 *     <li>{@link DoubleColumnReader}</li>
 *     <li>{@link IdColumnReader}</li>
 *     <li>{@link LongColumnReader}</li>
 *     <li>{@link StringColumnReader}</li>
 * </ul>
 */
public interface ColumnReader {

    /**
     * Whether the value of this {@link column.store.api.column.Column} of the current record is not {@code null}.
     *
     * <p> This method may block, until the value for the current record has been read.
     */
    boolean isPresent();

    /**
     * Whether the value of this {@link column.store.api.column.Column} of the current record is {@code null}.
     *
     * <p> This method may block, until the value for the current record has been read.
     */
    default boolean isNull() {
        return !isPresent();
    }
}
