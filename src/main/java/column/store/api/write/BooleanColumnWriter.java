package column.store.api.write;

public interface BooleanColumnWriter extends ColumnWriter {

    /**
     * Write {@code value} into the {@link column.store.api.column.BooleanColumn} of the current record.
     */
    void write(boolean value);
}
