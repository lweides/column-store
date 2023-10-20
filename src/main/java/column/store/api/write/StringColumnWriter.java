package column.store.api.write;

public non-sealed interface StringColumnWriter extends ColumnWriter {

    /**
     * Write {@code value} into the {@link column.store.api.column.StringColumn} of the current record.
     */
    void write(String value);
}
