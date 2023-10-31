package column.store.api.write;

public interface DoubleColumnWriter extends ColumnWriter {

    /**
     * Write {@code value} into the {@link column.store.api.column.DoubleColumn} of the current record.
     */
    void write(double value);
}
