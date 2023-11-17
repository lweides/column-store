package column.store.api.write;

import column.store.api.column.*;

import java.io.IOException;

import java.io.IOException;

public interface Writer extends AutoCloseable {

    /**
     * @return a {@link BooleanColumnWriter}, which writes the values of the given {@link BooleanColumn}.
     */
    BooleanColumnWriter of(BooleanColumn column);

    /**
     * @return a {@link DoubleColumnWriter}, which writes the values of the given {@link DoubleColumn}.
     */
    DoubleColumnWriter of(DoubleColumn column);

    /**
     * @return a {@link IdColumnWriter}, which writes the values of the given {@link IdColumn}.
     */
    IdColumnWriter of(IdColumn column);

    /**
     * @return a {@link LongColumnWriter}, which writes the values of the given {@link LongColumn}.
     */
    LongColumnWriter of(LongColumn column);

    /**
     * @return a {@link StringColumnWriter}, which writes the values of the given {@link StringColumn}.
     */
    StringColumnWriter of(StringColumn column);

    /**
     * Finish the current record. If a {@link Column} still has no value, {@link ColumnWriter#writeNull()} is called.
     *
     * <p> If a new {@link Column} is added while records are added, all previous record will have {@code null} for the
     * newly added {@link Column}.
     */
    void next() throws IOException;

    /**
     * Flushes the currently in-memory records to disk.
     */
    void flush() throws IOException;

    /**
     * Flushes the remaining records and releases any underlying resources.
     */
    @Override
    void close() throws IOException;
}
