package column.store.api.write;

public non-sealed interface IdColumnWriter extends ColumnWriter {

    /**
     * Write {@code value} into the {@link column.store.api.column.IdColumn} of the current record.
     */
    void write(byte[] value);
}
