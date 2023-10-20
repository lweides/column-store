package column.store.api.write;

import column.store.api.column.Column;

/**
 * A {@link ColumnWriter} is a typed writer for a single {@link Column}.
 * Supported writer types are:
 * <ul>
 *     <li>{@link BooleanColumnWriter}</li>
 *     <li>{@link DoubleColumnWriter}</li>
 *     <li>{@link IdColumnWriter}</li>
 *     <li>{@link LongColumnWriter}</li>
 *     <li>{@link StringColumnWriter}</li>
 * </ul>
 */
public sealed interface ColumnWriter permits BooleanColumnWriter, DoubleColumnWriter, IdColumnWriter, LongColumnWriter, StringColumnWriter {

    /**
     * Writes {@code null} into the {@link Column} of the current record.
     */
    void writeNull();
}
