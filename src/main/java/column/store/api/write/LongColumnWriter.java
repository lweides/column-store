package column.store.api.write;

public interface LongColumnWriter extends ColumnWriter {

    /**
     * Write {@code value} into the {@link column.store.api.column.LongColumn} of the current record.
     */
    void write(long value);
}
